package org.wikidata.wdtk.dump2neo;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Label;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.wikidata.wdtk.datamodel.interfaces.EntityDocumentProcessor;
import org.wikidata.wdtk.datamodel.interfaces.ItemDocument;
import org.wikidata.wdtk.datamodel.interfaces.MonolingualTextValue;
import org.wikidata.wdtk.datamodel.interfaces.PropertyDocument;

class EntityDumper implements EntityDocumentProcessor {

	public EntityDumper(BatchInserter inserter) {
		this.inserter = inserter;
	}

	public EntityDumper(BatchInserter inserter, String lang, boolean withDescription) {
		this(inserter);
		this.lang = lang;
		this.withDescription = withDescription;
	}
	
	private static final String ID = "id", LABEL = "label", DESCRIPTION = "description";
	
	private BatchInserter inserter;
	private String lang = "en";
	private boolean withDescription = true;
	
	private HashMap<String, Long> entities = new HashMap<String, Long>();
	public HashMap<String, Long> getEntities() {
		return entities;
	}

	@Override
	public void processItemDocument(ItemDocument itemDocument) {
		String id = itemDocument.getItemId().getId();
		MonolingualTextValue label = itemDocument.getLabels().get(lang);
		if (!isNullOrEmpty(label))
			createEntity(id, label, itemDocument, Labels.Entity);
	}

	@Override
	public void processPropertyDocument(PropertyDocument propertyDocument) {
		String id = propertyDocument.getEntityId().getId();
		MonolingualTextValue label = propertyDocument.getLabels().get(lang);
		if (!isNullOrEmpty(label))
			createEntity(id, label, null, Labels.Property);
	}

	private Map<String, Object> properties = new HashMap<>();
	
	private void createEntity(String id, MonolingualTextValue label, ItemDocument itemDocument, Label l) {
		properties.put(ID, id);
		properties.put(LABEL, label.getText());
		
		if(withDescription && itemDocument != null) {
			String description = getItemDescription(itemDocument);
			if(description != null)
				properties.put(DESCRIPTION, description);
			else 
				properties.remove(DESCRIPTION);
		}
		
		long nodeId = inserter.createNode(properties, l);
		entities.put(id, nodeId);
	}

	private String getItemDescription(ItemDocument itemDocument) {
		MonolingualTextValue description = 
			itemDocument.getDescriptions().get(lang);
		
		if (!isNullOrEmpty(description))
			return description.getText();
		
		return null;
	}
	
	private boolean isNullOrEmpty(MonolingualTextValue label) {
		return label == null || label.getText() == null || label.getText() == "";
	}
}