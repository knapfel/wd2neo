package org.wikidata.wdtk.dump2neo;

import java.io.IOException;

import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.wikidata.wdtk.examples.ExampleHelpers;

class Dump2Neo4j  {
	static BatchInserter inserter;
	
	public static void main(String[] args) throws IOException {
		ExampleHelpers.configureLogging();

		inserter = BatchInserters.inserter("target/wd-de-nodesc");
		
		try {			
			EntityDumper entityDumper = new EntityDumper(inserter, "de", false);
			ExampleHelpers.processEntitiesFromWikidataDump(entityDumper);
			
			StatementDumper statementDumper = new StatementDumper(inserter, entityDumper.getEntities());
			ExampleHelpers.processEntitiesFromWikidataDump(statementDumper);
		} finally {
			inserter.shutdown();
		}
	}
}
