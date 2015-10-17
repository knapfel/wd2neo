package org.wikidata.wdtk.dump2neo;

import java.util.HashMap;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.wikidata.wdtk.datamodel.interfaces.EntityDocumentProcessor;
import org.wikidata.wdtk.datamodel.interfaces.ItemDocument;
import org.wikidata.wdtk.datamodel.interfaces.MonolingualTextValue;
import org.wikidata.wdtk.datamodel.interfaces.PropertyDocument;

/**
 * Adds wikidata entities to Neo4j, keeps a map of entity id to
 * Neo4j node id.
 * 
 * Stores only entities that have an English label.
 * 
 * @author jens
 *
 */
class EntityDumper implements EntityDocumentProcessor {

	public EntityDumper(GraphDatabaseService graphDb, ITxKeeper txKeeper) {
		this.graphDb = graphDb;
		this.txKeeper = txKeeper;
	}

	private GraphDatabaseService graphDb;
	private ITxKeeper txKeeper;

	private Runtime runtime = Runtime.getRuntime();

	private HashMap<String, Long> entities = new HashMap<String, Long>();
	private HashMap<String, String> propertyLabels = new HashMap<String, String>();

	
	public HashMap<String, Long> getEntities() {
		return entities;
	}

	public HashMap<String, String> getPropertyLables() {
		return propertyLabels;
	}

	
	@Override
	public void processItemDocument(ItemDocument itemDocument) {
		String id = itemDocument.getItemId().getId();

		MonolingualTextValue label = itemDocument.getLabels().get("de");
		if (label == null || label.getText() == null || label.getText() == "")
			return;

		Node entity = createEntity(id, label, Labels.Entity);
		//addItemDescription(itemDocument, entity);
	}

	@Override
	public void processPropertyDocument(PropertyDocument propertyDocument) {
		String id = propertyDocument.getEntityId().getId();
		MonolingualTextValue label = propertyDocument.getLabels().get("de");
		if (label == null || label.getText() == null || label.getText() == "")
			return;

		createEntity(id, label, Labels.Property);
		propertyLabels.put(id, label.getText());
	}

	private Node createEntity(String id, MonolingualTextValue label, Label l) {
		Node entity = graphDb.createNode(l);
		entities.put(id, entity.getId());
		entity.setProperty("id", id);
		entity.setProperty("label", label.getText());

		if (txKeeper.checkCommit()) {
			logProgress();
		}
		return entity;
	}
	
	private void addItemDescription(ItemDocument itemDocument, Node entity) {
		MonolingualTextValue description = 
			itemDocument.getDescriptions().get("en");
		
		if (description != null) {
			String descriptionText = description.getText();
			if (descriptionText != null && descriptionText != "")
				entity.setProperty("description", description.getText());
		}
	}

	void logProgress() {
		Logger logger = Logger.getRootLogger();
		if (!logger.isInfoEnabled())
			return;

		String message = entities.size()
				+ " known entities, "
				+ (int) (((double) (runtime.totalMemory() - runtime
						.freeMemory()) / (double) runtime.maxMemory()) * 100d)
				+ " % of the heap used.";
		logger.log(Level.INFO, message);
	}
}