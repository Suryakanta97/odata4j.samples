package org.odata4j.appengine;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.core4j.Enumerable;
import org.core4j.Func1;
import org.joda.time.LocalDateTime;
import org.odata4j.core.ODataConstants;
import org.odata4j.core.OEntities;
import org.odata4j.core.OEntity;
import org.odata4j.core.OEntityId;
import org.odata4j.core.OEntityKey;
import org.odata4j.core.OExtension;
import org.odata4j.core.OFunctionParameter;
import org.odata4j.core.OLink;
import org.odata4j.core.OProperties;
import org.odata4j.core.OProperty;
import org.odata4j.edm.EdmDataServices;
import org.odata4j.edm.EdmEntityContainer;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.edm.EdmEntityType;
import org.odata4j.edm.EdmFunctionImport;
import org.odata4j.edm.EdmProperty;
import org.odata4j.edm.EdmSchema;
import org.odata4j.edm.EdmSimpleType;
import org.odata4j.edm.EdmType;
import org.odata4j.expression.AndExpression;
import org.odata4j.expression.BinaryCommonExpression;
import org.odata4j.expression.BoolCommonExpression;
import org.odata4j.expression.EntitySimpleProperty;
import org.odata4j.expression.EqExpression;
import org.odata4j.expression.Expression;
import org.odata4j.expression.GeExpression;
import org.odata4j.expression.GtExpression;
import org.odata4j.expression.LeExpression;
import org.odata4j.expression.LiteralExpression;
import org.odata4j.expression.LtExpression;
import org.odata4j.expression.NeExpression;
import org.odata4j.expression.OrderByExpression;
import org.odata4j.expression.OrderByExpression.Direction;
import org.odata4j.producer.BaseResponse;
import org.odata4j.producer.CountResponse;
import org.odata4j.producer.EntitiesResponse;
import org.odata4j.producer.EntityIdResponse;
import org.odata4j.producer.EntityQueryInfo;
import org.odata4j.producer.EntityResponse;
import org.odata4j.producer.InlineCount;
import org.odata4j.producer.ODataProducer;
import org.odata4j.producer.QueryInfo;
import org.odata4j.producer.Responses;
import org.odata4j.producer.edm.MetadataProducer;
import org.odata4j.exceptions.NotFoundException;

import com.google.appengine.api.datastore.DataTypeUtils;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.SortDirection;
import com.google.appengine.api.datastore.ShortBlob;
import com.google.appengine.api.datastore.Text;

public class DatastoreProducer implements ODataProducer {

  public static final String ID_PROPNAME = "id";
  private static final String CONTAINER_NAME = "Container";

  @SuppressWarnings("unchecked")
  private static final Set<EdmType> SUPPORTED_TYPES = Enumerable.create(EdmSimpleType.BOOLEAN, EdmSimpleType.BYTE, EdmSimpleType.STRING,
      EdmSimpleType.INT16, EdmSimpleType.INT32, EdmSimpleType.INT64, EdmSimpleType.SINGLE, EdmSimpleType.DOUBLE, EdmSimpleType.DATETIME,
      EdmSimpleType.BINARY // only up to 500 bytes MAX_SHORT_BLOB_PROPERTY_LENGTH
      ).cast(EdmType.class).toSet();

  private final EdmDataServices metadata;
  private final String namespace;
  private final DatastoreService datastore;

  public DatastoreProducer(String namespace, List<String> kinds) {
    this.namespace = namespace;
    this.metadata = buildMetadata(kinds);
    this.datastore = DatastoreServiceFactory.getDatastoreService();
  }

  @Override
  public EdmDataServices getMetadata() {
    return metadata;
  }

  private EdmDataServices buildMetadata(List<String> kinds) {

    List<EdmSchema.Builder> schemas = new ArrayList<EdmSchema.Builder>();
    List<EdmEntityContainer.Builder> containers = new ArrayList<EdmEntityContainer.Builder>();
    List<EdmEntitySet.Builder> entitySets = new ArrayList<EdmEntitySet.Builder>();
    List<EdmEntityType.Builder> entityTypes = new ArrayList<EdmEntityType.Builder>();

    List<EdmProperty.Builder> properties = new ArrayList<EdmProperty.Builder>();
    properties.add(EdmProperty.newBuilder(ID_PROPNAME).setType(EdmSimpleType.INT64));

    for (String kind : kinds) {
      EdmEntityType.Builder eet = EdmEntityType.newBuilder()
          .setNamespace(namespace)
          .setName(kind)
          .addKeys(ID_PROPNAME)
          .addProperties(properties);
      EdmEntitySet.Builder ees = EdmEntitySet.newBuilder().setName(kind).setEntityType(eet);
      entitySets.add(ees);
      entityTypes.add(eet);
    }

    EdmEntityContainer.Builder container = EdmEntityContainer.newBuilder().setName(CONTAINER_NAME).setIsDefault(true).addEntitySets(entitySets);
    containers.add(container);

    EdmSchema.Builder schema = EdmSchema.newBuilder().setNamespace(namespace).addEntityTypes(entityTypes).addEntityContainers(containers);
    schemas.add(schema);
    EdmDataServices.Builder metadata = EdmDataServices.newBuilder().setVersion(ODataConstants.DATA_SERVICE_VERSION).addSchemas(schemas);
    return metadata.build();
  }

  @Override
  public void close() {
    // noop
  }

  @Override
  public EntityResponse getEntity(String entitySetName, OEntityKey entityKey, EntityQueryInfo queryInfo) {
    EdmEntitySet ees = metadata.getEdmEntitySet(entitySetName);
    Entity e = findEntity(entitySetName, entityKey);
    checkNotFound(entitySetName, entityKey, e);

    return Responses.entity(toOEntity(ees, e));
  }

  @Override
  public EntitiesResponse getEntities(String entitySetName, QueryInfo queryInfo) {
    final EdmEntitySet ees = metadata.getEdmEntitySet(entitySetName);
    Query q = new Query(entitySetName);
    if (queryInfo.filter != null)
      applyFilter(q, queryInfo.filter);
    if (queryInfo.orderBy != null && queryInfo.orderBy.size() > 0)
      applySort(q, queryInfo.orderBy);
    PreparedQuery pq = datastore.prepare(q);

    Integer inlineCount = queryInfo.inlineCount == InlineCount.ALLPAGES ? pq.countEntities(FetchOptions.Builder.withDefaults()) : null;

    FetchOptions options = null;
    if (queryInfo.top != null)
      options = FetchOptions.Builder.withLimit(queryInfo.top);
    if (queryInfo.skip != null)
      options = options == null ? FetchOptions.Builder.withOffset(queryInfo.skip) : options.offset(queryInfo.skip);

    Iterable<Entity> iter = options == null ? pq.asIterable() : pq.asIterable(options);

    List<OEntity> entities = Enumerable.create(iter).select(new Func1<Entity, OEntity>() {
      public OEntity apply(Entity input) {
        return toOEntity(ees, input);
      }
    }).toList();

    return Responses.entities(entities, ees, inlineCount, null);
  }

  @Override
  public EntityResponse createEntity(String entitySetName, OEntity entity) {
    EdmEntitySet ees = metadata.getEdmEntitySet(entitySetName);
    String kind = ees.getType().getName();

    Entity e = new Entity(kind);
    applyProperties(e, entity.getProperties());

    datastore.put(e);

    return Responses.entity(toOEntity(ees, e));
  }

  @Override
  public void deleteEntity(String entitySetName, OEntityKey entityKey) {
    EdmEntitySet ees = metadata.getEdmEntitySet(entitySetName);
    String kind = ees.getType().getName();

    long id = Long.parseLong(entityKey.asSingleValue().toString());
    datastore.delete(KeyFactory.createKey(kind, id));
  }

  @Override
  public void mergeEntity(String entitySetName, OEntity entity) {
    OEntityKey entityKey = entity.getEntityKey();
    Entity e = findEntity(entitySetName, entity.getEntityKey());
    checkNotFound(entitySetName, entityKey, e);

    applyProperties(e, entity.getProperties());
    datastore.put(e);
  }

  @Override
  public void updateEntity(String entitySetName, OEntity entity) {
    OEntityKey entityKey = entity.getEntityKey();
    Entity e = findEntity(entitySetName, entityKey);
    checkNotFound(entitySetName, entityKey, e);

    // clear existing props
    for (String name : e.getProperties().keySet())
      e.removeProperty(name);

    applyProperties(e, entity.getProperties());
    datastore.put(e);
  }

  private static void checkNotFound(String entitySetName, OEntityKey entityKey, Entity e) {
    if (e == null) {
      throw new NotFoundException("entity " + entitySetName + " with key " + entityKey + " not found!");
    }
  }

  private static OEntity toOEntity(EdmEntitySet ees, Entity entity) {
    final List<OProperty<?>> properties = new ArrayList<OProperty<?>>();
    properties.add(OProperties.int64(ID_PROPNAME, entity.getKey().getId()));

    for (String name : entity.getProperties().keySet()) {
      Object propertyValue = entity.getProperty(name);
      if (propertyValue == null)
        continue;

      if (propertyValue instanceof String) {
        properties.add(OProperties.string(name, (String) propertyValue));
      } else if (propertyValue instanceof Integer) {
        properties.add(OProperties.int32(name, (Integer) propertyValue));
      } else if (propertyValue instanceof Long) {
        properties.add(OProperties.int64(name, (Long) propertyValue));
      } else if (propertyValue instanceof Short) {
        properties.add(OProperties.int16(name, (Short) propertyValue));
      } else if (propertyValue instanceof Boolean) {
        properties.add(OProperties.boolean_(name, (Boolean) propertyValue));
      } else if (propertyValue instanceof Float) {
        properties.add(OProperties.single(name, (Float) propertyValue));
      } else if (propertyValue instanceof Double) {
        properties.add(OProperties.double_(name, (Double) propertyValue));
      } else if (propertyValue instanceof Date) {
        properties.add(OProperties.datetime(name, (Date) propertyValue));
      } else if (propertyValue instanceof Byte) {
        properties.add(OProperties.sbyte_(name, (Byte) propertyValue));
      }

      // Ordinary Java strings stored as properties in Entity objects are limited to 500 characters (DataTypeUtils.MAX_STRING_PROPERTY_LENGTH)
      // Text are unlimited, but unindexed
      else if (propertyValue instanceof Text) {
        Text text = (Text) propertyValue;
        properties.add(OProperties.string(name, text.getValue()));
      } else if (propertyValue instanceof ShortBlob) {
        ShortBlob sb = (ShortBlob) propertyValue;
        properties.add(OProperties.binary(name, sb.getBytes()));
      } else {
        throw new UnsupportedOperationException(propertyValue.getClass().getName());
      }
    }

    return OEntities.create(ees, OEntityKey.infer(ees, properties), properties, new ArrayList<OLink>());
  }

  private void applyProperties(Entity e, List<OProperty<?>> properties) {
    for (OProperty<?> prop : properties) {
      EdmType type = prop.getType();
      if (!SUPPORTED_TYPES.contains(type)) {
        throw new UnsupportedOperationException("EdmType not supported: " + type);
      }

      Object value = prop.getValue();
      if (type.equals(EdmSimpleType.STRING)) {
        String sValue = (String) value;
        if (sValue != null && sValue.length() > DataTypeUtils.MAX_STRING_PROPERTY_LENGTH) {
          value = new Text(sValue);
        }
      } else if (type.equals(EdmSimpleType.BINARY)) {
        byte[] bValue = (byte[]) value;
        if (bValue != null) {
          if (bValue.length > DataTypeUtils.MAX_SHORT_BLOB_PROPERTY_LENGTH) {
            throw new RuntimeException("Bytes " + bValue.length + " exceeds the max supported length " + DataTypeUtils.MAX_SHORT_BLOB_PROPERTY_LENGTH);
          }
          value = new ShortBlob(bValue);
        }
      } else if (type.equals(EdmSimpleType.DATETIME)) {
        LocalDateTime dValue = (LocalDateTime) value;
        if (dValue != null) {
          value = dValue.toDateTime().toDate(); // TODO review
        }
      }
      e.setProperty(prop.getName(), value);
    }
  }

  private Entity findEntity(String entitySetName, OEntityKey entityKey) {
    EdmEntitySet ees = metadata.getEdmEntitySet(entitySetName);
    String kind = ees.getType().getName();

    long id = Long.parseLong(entityKey.asSingleValue().toString());
    try {
      return datastore.get(KeyFactory.createKey(kind, id));
    } catch (EntityNotFoundException e) {
      return null;
    }
  }

  private void applySort(Query q, List<OrderByExpression> orderBy) {
    for (OrderByExpression ob : orderBy) {
      if (!(ob.getExpression() instanceof EntitySimpleProperty)) {
        throw new UnsupportedOperationException("Appengine only supports simple property expressions");
      }
      String propName = ((EntitySimpleProperty) ob.getExpression()).getPropertyName();
      if (propName.equals(ID_PROPNAME)) {
        propName = Entity.KEY_RESERVED_PROPERTY;
      }
      q.addSort(propName, ob.getDirection() == Direction.ASCENDING ? SortDirection.ASCENDING : SortDirection.DESCENDING);
    }
  }

  private void applyFilter(Query q, BoolCommonExpression filter) {
    // appengine supports simple filterpredicates (name op value)

    // one filter
    if (filter instanceof EqExpression)
      applyFilter(q, (EqExpression) filter, FilterOperator.EQUAL);
    else if (filter instanceof NeExpression)
      applyFilter(q, (NeExpression) filter, FilterOperator.NOT_EQUAL);
    else if (filter instanceof GtExpression)
      applyFilter(q, (GtExpression) filter, FilterOperator.GREATER_THAN);
    else if (filter instanceof GeExpression)
      applyFilter(q, (GeExpression) filter, FilterOperator.GREATER_THAN_OR_EQUAL);
    else if (filter instanceof LtExpression)
      applyFilter(q, (LtExpression) filter, FilterOperator.LESS_THAN);
    else if (filter instanceof LeExpression)
      applyFilter(q, (LeExpression) filter, FilterOperator.LESS_THAN_OR_EQUAL);

    // and filter
    else if (filter instanceof AndExpression) {
      AndExpression e = (AndExpression) filter;
      applyFilter(q, e.getLHS());
      applyFilter(q, e.getRHS());
    }

    else
      throw new UnsupportedOperationException("Appengine only supports simple property expressions");
  }

  private void applyFilter(Query q, BinaryCommonExpression e, FilterOperator op) {
    if (!(e.getLHS() instanceof EntitySimpleProperty))
      throw new UnsupportedOperationException("Appengine only supports simple property expressions");
    if (!(e.getRHS() instanceof LiteralExpression))
      throw new UnsupportedOperationException("Appengine only supports simple property expressions");

    EntitySimpleProperty lhs = (EntitySimpleProperty) e.getLHS();
    LiteralExpression rhs = (LiteralExpression) e.getRHS();

    String propName = lhs.getPropertyName();
    Object propValue = Expression.literalValue(rhs);
    if (propName.equals(ID_PROPNAME)) {
      propName = Entity.KEY_RESERVED_PROPERTY;
      propValue = KeyFactory.createKey(q.getKind(), (Long) propValue);
    }

    q.addFilter(propName, op, propValue);
  }

  // UNIMPLEMENTED

  @Override
  public EntityResponse createEntity(String entitySetName, OEntityKey entityKey, String navProp, OEntity entity) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BaseResponse getNavProperty(
      String entitySetName,
      OEntityKey entityKey,
      String navProp,
      QueryInfo queryInfo) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MetadataProducer getMetadataProducer() {
    throw new UnsupportedOperationException();
  }

  @Override
  public EntityIdResponse getLinks(OEntityId sourceEntity, String targetNavProp) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createLink(OEntityId sourceEntity, String targetNavProp, OEntityId targetEntity) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateLink(OEntityId sourceEntity, String targetNavProp, OEntityKey oldTargetEntityKey, OEntityId newTargetEntity) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteLink(OEntityId sourceEntity, String targetNavProp, OEntityKey targetEntityKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BaseResponse callFunction(EdmFunctionImport name, Map<String, OFunctionParameter> params, QueryInfo queryInfo) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CountResponse getEntitiesCount(String entitySetName, QueryInfo queryInfo) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CountResponse getNavPropertyCount(String entitySetName, OEntityKey entityKey, String navProp, QueryInfo queryInfo) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <TExtension extends OExtension<ODataProducer>> TExtension findExtension(Class<TExtension> arg0) {
    return null;
  }

}
