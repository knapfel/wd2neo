package org.wikidata.wdtk.dump2neo;

import org.neo4j.graphdb.Transaction;

class TxKeeper implements ITxKeeper {
	public TxKeeper(Transaction tx) {
		this.tx = tx;
	}
	
	public TxKeeper(Transaction tx, long batchSize) {
		this(tx);
		this.batchSize = batchSize;
	}
	
	
	private Transaction tx;
	private long progress = 0;
	private long batchSize = 5000;
	
	@Override
	public boolean checkCommit() {
		if(++progress % batchSize == 0) {
			tx.success();
			tx.close();
			tx = Dump2Neo4j.graphDb.beginTx();
			return true;
		} else {
			return false;
		}
	}
}