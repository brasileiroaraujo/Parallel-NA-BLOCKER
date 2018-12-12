package SparkNaBlocker.SparkNaBlocker;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.SparkSession;

import com.google.common.collect.Lists;

import DataStructures.Attribute;
import DataStructures.EntityProfile;
import DataStructures.Node;
import scala.Tuple2;
import tokens.KeywordGenerator;
import tokens.KeywordGeneratorImpl;

public class SparkNaBlockerMain {
	
	public static void main(String[] args) {
		SparkSession spark = SparkSession
			      .builder()
			      .appName("Spark NA-Blocker")
			      .master("local[3]")
			      .getOrCreate();
		
		ArrayList<EntityProfile> source = getDataSource(args[0], true);
		ArrayList<EntityProfile> target = getDataSource(args[1], false);
		
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
	    
	    Dataset<EntityProfile> dfSource = spark.createDataset(source, Encoders.javaSerialization(EntityProfile.class));
	    Dataset<EntityProfile> dfTarget = spark.createDataset(target, Encoders.javaSerialization(EntityProfile.class));
	    
	    Dataset<Tuple2<Integer, Node>> sourceNodes = dfSource.flatMap(new FlatMapFunction<EntityProfile, Tuple2<Integer, Node>>() {

			@Override
			public Iterator call(EntityProfile se) throws Exception {
				List<Tuple2<Integer, Node>> output = new ArrayList<Tuple2<Integer, Node>>();
				
				Set<Integer> cleanTokens = new HashSet<Integer>();
				
				for (Attribute att : se.getAttributes()) {
//					String[] tokens = gr.demokritos.iit.jinsect.utils.splitToWords(att.getValue());
					KeywordGenerator kw = new KeywordGeneratorImpl();
					for (String string : kw.generateKeyWords(att.getValue())) {
						cleanTokens.add(string.hashCode());
					}
				}
				
				Node node = new Node(se.getIdEntity(), cleanTokens, new HashSet<>(), se.isSource());
				
				for (Integer tk : cleanTokens) {
					node.setTokenTemporary(tk);
					output.add(new Tuple2<Integer, Node>(tk, node));
				}
				
				return output.iterator();
			}
		}, Encoders.tuple(Encoders.INT(), Encoders.javaSerialization(Node.class)));
	    
	    
	    Dataset<Tuple2<Integer, Node>> targetNodes = dfTarget.flatMap(new FlatMapFunction<EntityProfile, Tuple2<Integer, Node>>() {

			@Override
			public Iterator call(EntityProfile se) throws Exception {
				List<Tuple2<Integer, Node>> output = new ArrayList<Tuple2<Integer, Node>>();
				
				Set<Integer> cleanTokens = new HashSet<Integer>();
				
				for (Attribute att : se.getAttributes()) {
//					String[] tokens = gr.demokritos.iit.jinsect.utils.splitToWords(att.getValue());
					KeywordGenerator kw = new KeywordGeneratorImpl();
					for (String string : kw.generateKeyWords(att.getValue())) {
						cleanTokens.add(string.hashCode());
					}
				}
				
				Node node = new Node(se.getIdEntity(), cleanTokens, new HashSet<>(), se.isSource());
				
				for (Integer tk : cleanTokens) {
					node.setTokenTemporary(tk);
					output.add(new Tuple2<Integer, Node>(tk, node));
				}
				
				return output.iterator();
			}
		}, Encoders.tuple(Encoders.INT(), Encoders.javaSerialization(Node.class)));
	    
	    Dataset<Tuple2<Integer, Node>> allNodes = sourceNodes.union(targetNodes);
	    
	    KeyValueGroupedDataset<Integer, Tuple2<Integer, Node>> blocks = allNodes.groupByKey(new MapFunction<Tuple2<Integer, Node>, Integer>() {

			@Override
			public Integer call(Tuple2<Integer, Node> value) throws Exception {
				return value._1();
			}
		}, Encoders.INT());
	    
	    Dataset<Tuple2<Integer, Node>> graph = blocks.flatMapGroups(new FlatMapGroupsFunction<Integer, Tuple2<Integer, Node>, Tuple2<Integer, Node>>() {

			@Override
			public Iterator<Tuple2<Integer, Node>> call(Integer key, Iterator<Tuple2<Integer, Node>> values)
					throws Exception {
				List<Tuple2<Integer, Node>> entitiesToCompare = Lists.newArrayList(values);
				List<Tuple2<Integer, Node>> output = new ArrayList<Tuple2<Integer, Node>>();
				for (int i = 0; i < entitiesToCompare.size(); i++) {
					Node n1 = entitiesToCompare.get(i)._2();
					for (int j = i+1; j < entitiesToCompare.size(); j++) {
						Node n2 = entitiesToCompare.get(j)._2();
						//Only compare nodes from distinct sources and marked as new (avoid recompute comparisons)
						if (n1.isSource() != n2.isSource() && (n1.isMarked() || n2.isMarked())) {
							double similarity = calculateSimilarity(key, n1.getBlocks(), n2.getBlocks());
							if (similarity >= 0) {
								if (n1.isSource()) {
									n1.addNeighbor(new Tuple2<Integer, Double>(n2.getId(), similarity));
								} else {
									n2.addNeighbor(new Tuple2<Integer, Double>(n1.getId(), similarity));
								}
							}
							
						}
					}
				}
				
				for (Tuple2<Integer, Node> node : entitiesToCompare) {
					if (node._2().isSource()) {
						output.add(new Tuple2<Integer, Node>(node._2().getId(), node._2()));
					}
				}
				return output.iterator();
			}

			private double calculateSimilarity(Integer blockKey, Set<Integer> ent1, Set<Integer> ent2) {
				int maxSize = Math.max(ent1.size() - 1, ent2.size() - 1);
				Set<Integer> intersect = new HashSet<Integer>(ent1);
				intersect.retainAll(ent2);

				// MACOBI strategy
				if (!Collections.min(intersect).equals(blockKey)) {
					return -1;
				}

				if (maxSize > 0) {
					double x = (double) intersect.size() / maxSize;
					return x;
				} else {
					return 0;
				}
			}
		}, Encoders.tuple(Encoders.INT(), Encoders.javaSerialization(Node.class)));
	    
	    //A beginning block is generated, notice that we have all the Neighbors of each entity.
	    KeyValueGroupedDataset<Integer, Tuple2<Integer, Node>> weightedGraph = graph.groupByKey(new MapFunction<Tuple2<Integer, Node>, Integer>() {

			@Override
			public Integer call(Tuple2<Integer, Node> n) throws Exception {
				return n._1();
			}
		}, Encoders.INT());
	    
	    
	    //Execute the pruning removing the low edges (entities in the Neighbors list)
	    Dataset<String> prunedGraph = weightedGraph.mapGroups(new MapGroupsFunction<Integer, Tuple2<Integer, Node>, String>() {
	    	
	    	
	    	@Override
			public String call(Integer key, Iterator<Tuple2<Integer, Node>> values)
					throws Exception {
	    		List<Tuple2<Integer, Node>> nodes = IteratorUtils.toList(values);
				
				Node n1 = nodes.get(0)._2();//get the first node to merge with others.
				for (int j = 1; j < nodes.size(); j++) {
					Node n2 = nodes.get(j)._2();
					n1.addSetNeighbors(n2);
				}
				
				n1.pruning();
				return n1.getId() + "," + n1.toString();
			}


		}, Encoders.STRING());
	    
	    
	    prunedGraph.show();
	    

	}

	
	
	
	
	private static ArrayList<EntityProfile> getDataSource(String path, boolean isSource) {
        ArrayList<EntityProfile> EntityList = null;
        
		// reading the files
		ObjectInputStream ois1;
		try {
			ois1 = new ObjectInputStream(new FileInputStream(path));
			EntityList = (ArrayList<EntityProfile>) ois1.readObject();
			ois1.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		int id = 0;
		if (EntityList != null) {
			for (EntityProfile entityProfile : EntityList) {
				id++;
				entityProfile.setIdEntity(id);
				entityProfile.setIsSource(isSource);
			}
		}
		
		return EntityList;
		
	}

}

