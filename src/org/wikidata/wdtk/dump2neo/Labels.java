package org.wikidata.wdtk.dump2neo;

import org.neo4j.graphdb.Label;

enum Labels implements Label {
	Entity,
	Property
}