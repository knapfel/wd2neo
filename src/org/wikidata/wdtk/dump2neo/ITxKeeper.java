package org.wikidata.wdtk.dump2neo;

interface ITxKeeper {
	boolean checkCommit();
}