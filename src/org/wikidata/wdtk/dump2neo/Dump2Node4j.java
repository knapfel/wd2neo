package org.wikidata.wdtk.dump2neo;

import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.wikidata.wdtk.examples.ExampleHelpers;

class Dump2Neo4j  {
	static GraphDatabaseService graphDb;
	
	public static void main(String[] args) throws IOException {
		
		ExampleHelpers.configureLogging();

		graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(
				"target/wd-de-nodesc").newGraphDatabase();
		registerShutdownHook(graphDb);

		Transaction tx = graphDb.beginTx();
		try {
			TxKeeper txKeeper = new TxKeeper(tx, 50000);
			
			EntityDumper entityDumper = new EntityDumper(graphDb, txKeeper);
			ExampleHelpers.processEntitiesFromWikidataDump(entityDumper);
			
			StatementDumper statementDumper = 
					new StatementDumper(
							graphDb, entityDumper.getEntities(),
							entityDumper.getPropertyLables(), txKeeper);
			ExampleHelpers.processEntitiesFromWikidataDump(statementDumper);
		} finally {
			tx.success();
			tx.close();
		}
	}

	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		// Registers a shutdown hook for the Neo4j instance so that it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running application).
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}
}
