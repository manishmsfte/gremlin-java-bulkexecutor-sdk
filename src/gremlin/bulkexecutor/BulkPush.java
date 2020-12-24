package gremlin.bulkexecutor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import gremlin.core.EdgeType;
import gremlin.core.GraphElementType;
import gremlin.core.VertexType;

public class BulkPush<T> implements Runnable {
	static final int MAX_VERTEX_PER_REQUEST = 20;
	static final int MAX_EDGE_PER_REQUEST = 20;
	private static final int RETRY_COUNT = 4;
	private Client client;
	private Cluster cluster;
	private String graphName;
	private ArrayList<T> vertices;
	// private CountDownLatch latch;
	//private Map<String, LogModel> timeCapture;
	private String timeKeyValue;
	private GraphTraversalSource g;
	private String idFieldName;
	private static final Logger log = LogManager.getLogger(BulkPush.class);

	public BulkPush(Client cObj, String gName, Object object, String timeKey,
			String idFieldName) {
		client = cObj;
		graphName = gName;
		vertices = (ArrayList<T>) object;
		
		timeKeyValue = timeKey;
		this.idFieldName = idFieldName;
	}

	public BulkPush(Cluster cObj, String gName, Object object, String timeKey,
			String idFieldName) {
		cluster = cObj;
		graphName = gName;
		vertices = (ArrayList<T>) object;
		// latch = l;
		//timeCapture = bulkPushTime;
		timeKeyValue = timeKey;
		this.idFieldName = idFieldName;
	}

	@Override
	public void run() {

		int GlobalVertexCount = 0, globalEdgeCount = 0;
		long startTime = System.currentTimeMillis();

		g = AnonymousTraversalSource.traversal().withRemote(DriverRemoteConnection.using(cluster, graphName));

		GraphTraversal<Vertex, Vertex> gg = null;

		log.debug("GraphTraversalSource" + timeKeyValue + " StartTime:" + startTime + "End Time: "
				+ System.currentTimeMillis() + " Difference:" + (System.currentTimeMillis() - startTime));

		int vertexCount = 0, edgeCount = 0;
		// First iteration will provide a flag to demarcate starting point.
		Boolean firstIteration = true;
		Boolean success = false;
		int retryCount = 0;
		for (int vertexIteration = 0; vertexIteration < vertices.size(); vertexIteration++) {
			T vertex = vertices.get(vertexIteration);
			Field[] fields = vertex.getClass().getFields();
			Map<String, Object> fieldValues = ((VertexType) vertex).generateMap();
			// Get Label to add vertex object
			String label = fieldValues.get("label").toString();
			if (firstIteration) {
				gg = g.addV(label);

			} else {
				gg = gg.addV(label);
			}

			// Now populate other properties of the vertex
			for (Field field : fields) {
				try {
					if (field.getAnnotations().length == 0 && field.getName() != "label") {
						String fieldName = field.getName();

						// schema discovery at runtime
						Object fieldValue = fieldValues.get(fieldName);
						gg.property(fieldName, fieldValue);
					}

				} catch (Exception e) {
					log.error(e.getMessage() + " " + e.getStackTrace());
				}
			}

			firstIteration = false;
			vertexCount++;
			if (vertexCount == MAX_VERTEX_PER_REQUEST) {
				success = processGraphObject(gg, timeKeyValue + "-" + GlobalVertexCount + "-pervertex", retryCount);
				vertexCount = 0;

				firstIteration = true;
				gg = null;
				if (!success & retryCount < RETRY_COUNT) {
					vertexIteration -= MAX_VERTEX_PER_REQUEST;

					retryCount++;
				} else
					retryCount = 0;
			}
			GlobalVertexCount++;
		}

		// if there are some vertex still remains then add them.
		if (vertexCount != 0) {
			success = processGraphObject(gg, timeKeyValue + " final-pervertex", retryCount);
			gg = null;
			vertexCount = 0;
			firstIteration = true;
			retryCount = 0;
		}

		// Now add Spouse Edge for all the newly added edges
		edgeCount = 0;
		firstIteration = true;
		GraphTraversal<Vertex, Edge> ge = null;
		retryCount = 0;
		// c = 0;
		for (int edgeIteration = 0; edgeIteration < vertices.size(); edgeIteration++) {
			T vertex = vertices.get(edgeIteration);
			for (Field f : vertex.getClass().getDeclaredFields()) {
				for (Annotation an : f.getDeclaredAnnotations()) {
					if (an instanceof GraphElementType) {
						if (f.getType().getSimpleName().contentEquals("ArrayList")) {
							ArrayList<EdgeType> edgeTypes = null;
							try {
								edgeTypes = (ArrayList<EdgeType>) f.get(vertex);

							} catch (Exception e) {
							log.error(e.getMessage() + " " + e.getStackTrace());
							}

							for (EdgeType et : edgeTypes) {
								// Edge generation logic is refactored
								ge = processEdge(vertex, g, ge, et, firstIteration, ((GraphElementType) an).label(),
										idFieldName);
								firstIteration = false;
								edgeCount++;
								globalEdgeCount++;
								if (edgeCount >= MAX_EDGE_PER_REQUEST) {
									firstIteration = true;
									success = processGraphObject(ge, timeKeyValue + "-peredge " + globalEdgeCount,
											retryCount);
									if (!success & retryCount < RETRY_COUNT) {
										edgeIteration -= MAX_VERTEX_PER_REQUEST;

										retryCount++;
									} else
										retryCount = 0;
									ge = null;
									edgeCount = 0;
								}
							}
						}

						if (f.getType().getSuperclass().getSimpleName().equals("EdgeType")
								|| f.getType().getSimpleName().equals("EdgeType")) {
							EdgeType edgeType = null;
							try {
								edgeType = (EdgeType) f.get(vertex);
							} catch (Exception e) {
								log.error(e.getMessage() + " " + e.getStackTrace());
							}
							if (edgeType != null) {
									ge = processEdge(vertex, g, ge, edgeType, firstIteration,
										((GraphElementType) an).label(), idFieldName);

								edgeCount++;
								globalEdgeCount++;
							}
							if (edgeCount >= MAX_EDGE_PER_REQUEST) {

								success = processGraphObject(ge, timeKeyValue + "-peredge" + globalEdgeCount,
										retryCount);
								if (!success & retryCount < RETRY_COUNT) {
									edgeIteration -= MAX_VERTEX_PER_REQUEST;

									retryCount++;
								} else
									retryCount = 0;

								edgeCount = 0;
								firstIteration = true;
								ge = null;
							}
						}

					}
				}
				if (edgeCount >= MAX_EDGE_PER_REQUEST) {
					edgeCount = 0;
					firstIteration = true;
					processGraphObject(ge, timeKeyValue + "-peredge " + globalEdgeCount, retryCount);
				}
			}

		}
		if (ge != null && edgeCount != 0) {
			processGraphObject(ge, timeKeyValue + "-peredge Final", retryCount);
		}
		vertices.clear();
		vertices = null;
		log.debug(timeKeyValue + " StartTime:" + startTime + "End Time: " + System.currentTimeMillis() + " Difference:"
				+ (System.currentTimeMillis() - startTime));
		try {
			g.close();
			g=null;
			log.debug("Thread - " + timeKeyValue + " is finished. and total time taken is " + 
					(System.currentTimeMillis() - startTime)
					+ "ms\n No. of Vertex:" + GlobalVertexCount + " No of Edges: " + globalEdgeCount);
		} catch (Exception e) {
			log.error(e.getMessage() + " " + e.getStackTrace());
		}
		
	}

	/**
	 * @param ge
	 */
	private Boolean processGraphObject(GraphTraversal ge, String title, int retryCount) {
		long c = System.currentTimeMillis();
		Bytecode msg = ge.asAdmin().getBytecode();

		boolean succeeded = false;
		// for (int i = 0; i <= RETRY_COUNT && succeeded == false; i++) {
		try {
			ge.iterate();
			succeeded = true;
			ge = null;
			log.debug(title + " " + timeKeyValue + " StartTime:" + c + "End Time: " + System.currentTimeMillis()
					+ " Difference:" + (System.currentTimeMillis() - c + " msg:" + msg));
		} catch (Exception e) {
			succeeded = false;

			log.error(title + " retry count:" + retryCount + " " + timeKeyValue + " Success:" + succeeded
					+ " StartTime:" + c + "End Time: " + System.currentTimeMillis() + " Difference:"
					+ (System.currentTimeMillis() - c + " Stack trace" + e.getMessage() + "stack Trace" + e.getStackTrace()
							+ "msg:" + msg));

		} finally {
			ge = null;
		}

		return succeeded;

	}

	private GraphTraversal<Vertex, Edge> processEdge(T vertex, GraphTraversalSource g, GraphTraversal<Vertex, Edge> ge,
			EdgeType et, Boolean firstIteration, String label, String idFieldName) {

		if (et != null) {
			VertexType vertexType = (VertexType) vertex;
			if (firstIteration) {

				ge = g.V().has(idFieldName, (vertexType.customerId)).addE(label).to(g.V().has(idFieldName, et.id));
				firstIteration = false;
			} else {
				ge = ge.V().has(idFieldName, (vertexType.customerId)).addE(label).to(g.V().has(idFieldName, et.id));

			}
			// System.out.println(vertexType.customerId);
			Map<String, Object> e = et.generateMap();

			for (Map.Entry<String, Object> entry : e.entrySet()) {
				ge = ge.property(entry.getKey(), entry.getValue());
			}
		}
		return ge;
	}
}
