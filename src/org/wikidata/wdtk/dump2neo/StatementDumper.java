package org.wikidata.wdtk.dump2neo;

import java.util.HashMap;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.wikidata.wdtk.datamodel.interfaces.EntityDocumentProcessor;
import org.wikidata.wdtk.datamodel.interfaces.ItemDocument;
import org.wikidata.wdtk.datamodel.interfaces.ItemIdValue;
import org.wikidata.wdtk.datamodel.interfaces.PropertyDocument;
import org.wikidata.wdtk.datamodel.interfaces.Statement;
import org.wikidata.wdtk.datamodel.interfaces.StatementGroup;
import org.wikidata.wdtk.datamodel.interfaces.Value;
import org.wikidata.wdtk.datamodel.interfaces.ValueSnak;

class StatementDumper implements EntityDocumentProcessor {

	public StatementDumper(
			BatchInserter inserter,
			HashMap<String, Long> entities) {
		
		this.entities = entities;
		this.inserter = inserter;
	}

	private BatchInserter inserter;
	private HashMap<String, Long> entities;

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
					
					inserter.createRelationship(subjectId, objectId, 
							DynamicRelationshipType.withName(predicateItemId), null);					
				} else {
					inserter.setNodeProperty(subjectId, predicateItemId, v.toString());
				}
			}
		}
	}

	@Override
	public void processPropertyDocument(PropertyDocument propertyDocument) {
		return;
	}
}
