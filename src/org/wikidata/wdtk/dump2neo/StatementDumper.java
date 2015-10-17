package org.wikidata.wdtk.dump2neo;

import java.util.HashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.wikidata.wdtk.datamodel.interfaces.EntityDocumentProcessor;
import org.wikidata.wdtk.datamodel.interfaces.ItemDocument;
import org.wikidata.wdtk.datamodel.interfaces.ItemIdValue;
import org.wikidata.wdtk.datamodel.interfaces.PropertyDocument;
import org.wikidata.wdtk.datamodel.interfaces.Statement;
import org.wikidata.wdtk.datamodel.interfaces.StatementGroup;
import org.wikidata.wdtk.datamodel.interfaces.Value;
import org.wikidata.wdtk.datamodel.interfaces.ValueSnak;


/**
 * Add statements from wikidata items to Neo4j. ValueSnaks only for now.
 * Creates a relationship if the statement has an ItemIdValue 
 * or adds a string valued item property else.
 */
class StatementDumper implements EntityDocumentProcessor {

	public StatementDumper(
			GraphDatabaseService graphDb,
			HashMap<String, Long> entities, 
			HashMap<String, String> propertyLabels, 
			ITxKeeper txKeeper) {
		
		this.graphDb = graphDb;
		this.entities = entities;
		this.propertyLabels = propertyLabels;
		this.txKeeper = txKeeper;
	}

	private GraphDatabaseService graphDb;
	private HashMap<String, Long> entities;
	private HashMap<String, String> propertyLabels;
	private ITxKeeper txKeeper;

	@Override
	public void processItemDocument(ItemDocument itemDocument) {
		Long subjectId = entities.get(itemDocument.getItemId().getId());
		if (subjectId == null)
			return;

		for (StatementGroup sg : itemDocument.getStatementGroups()) {
			if (sg.getProperty() == null || sg.getProperty().getId() == null
					|| sg.getProperty().getId() == "")
				continue;

			String predicateItemId = sg.getProperty().getId();
			if (predicateItemId == null || predicateItemId == "")
				continue;

			for (Statement s : sg.getStatements()) {
				if (!(s.getClaim().getMainSnak() instanceof ValueSnak))
					continue;

				Value v = ((ValueSnak) s.getClaim().getMainSnak()).getValue();
				if (v instanceof ItemIdValue) {
					String oId = ((ItemIdValue) v).getId();

					if (oId == null || oId == "")
						continue;

					Long objectId = entities.get(oId);
					if (objectId == null)
						continue;

					Node subject = graphDb.getNodeById(subjectId);
					Node object = graphDb.getNodeById(objectId);
					
					Relationship s2o = subject.createRelationshipTo(object,
							DynamicRelationshipType.withName(predicateItemId));
					s2o.setProperty("id", predicateItemId);
					String predicateLabel = propertyLabels.get(predicateItemId);
					if(predicateLabel != null) {
						s2o.setProperty("label", predicateLabel);
					} else {
						Logger.getRootLogger().log(Level.INFO, "predicate " + predicateLabel + " / " + predicateItemId + " not found.");
					}

				} else {
					graphDb.getNodeById(subjectId).setProperty(predicateItemId, v.toString());
				}
				txKeeper.checkCommit();
			}
		}
	}

	@Override
	public void processPropertyDocument(PropertyDocument propertyDocument) {
		return;
	}
}
