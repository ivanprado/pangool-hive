package com.datasalt.pangool.hive;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

import java.nio.ByteBuffer;
import java.util.*;

public class PangoolHiveSerDe implements SerDe {

  String schemaName = "from_hive";
  List<String> columnNames;
  List<TypeInfo> columnTypes;
  private StructObjectInspector rowOI;
  private ArrayList<Object> row;
  private Object[] outputFields;

  // translates row idx to tuple idx
  private int[] rowToTuple;

  // Tuples reused by the serializer
  private Tuple tmpTuple = null;
  private TupleWritable tupleWrapper = null;

  protected static final HashMap<TypeInfo, Schema.Field.Type> hiveToPangoolConversion =
      new HashMap<TypeInfo, Schema.Field.Type>();
  static {
    hiveToPangoolConversion.put(TypeInfoFactory.byteTypeInfo, Schema.Field.Type.INT);
    hiveToPangoolConversion.put(TypeInfoFactory.shortTypeInfo, Schema.Field.Type.INT);
    hiveToPangoolConversion.put(TypeInfoFactory.intTypeInfo, Schema.Field.Type.INT);
    hiveToPangoolConversion.put(TypeInfoFactory.longTypeInfo, Schema.Field.Type.LONG);
    hiveToPangoolConversion.put(TypeInfoFactory.booleanTypeInfo, Schema.Field.Type.BOOLEAN);
    hiveToPangoolConversion.put(TypeInfoFactory.floatTypeInfo, Schema.Field.Type.FLOAT);
    hiveToPangoolConversion.put(TypeInfoFactory.doubleTypeInfo, Schema.Field.Type.DOUBLE);
    hiveToPangoolConversion.put(TypeInfoFactory.stringTypeInfo, Schema.Field.Type.STRING);
    hiveToPangoolConversion.put(TypeInfoFactory.binaryTypeInfo, Schema.Field.Type.BYTES);
  }

  // Allowed conversions between Pangool and Hive
  protected static final HashMap<Schema.Field.Type, TypeInfo> pangoolToHiveConversion =
      new HashMap<Schema.Field.Type, TypeInfo>();

  static {
    pangoolToHiveConversion.put(Schema.Field.Type.INT, TypeInfoFactory.intTypeInfo);
    pangoolToHiveConversion.put(Schema.Field.Type.LONG, TypeInfoFactory.longTypeInfo);
    pangoolToHiveConversion.put(Schema.Field.Type.BOOLEAN, TypeInfoFactory.booleanTypeInfo);
    pangoolToHiveConversion.put(Schema.Field.Type.FLOAT, TypeInfoFactory.floatTypeInfo);
    pangoolToHiveConversion.put(Schema.Field.Type.DOUBLE, TypeInfoFactory.doubleTypeInfo);
    pangoolToHiveConversion.put(Schema.Field.Type.STRING, TypeInfoFactory.stringTypeInfo);
    pangoolToHiveConversion.put(Schema.Field.Type.BYTES, TypeInfoFactory.binaryTypeInfo);
    pangoolToHiveConversion.put(Schema.Field.Type.ENUM, TypeInfoFactory.intTypeInfo);
  }

  @Override
  public void initialize(Configuration entries, Properties properties) throws SerDeException {
    rowToTuple = null;
    // We can get the table definition from properties.

    // Read the configuration parameters
    schemaName = properties.getProperty("schema.name");
    String columnNameProperty = properties.getProperty(Constants.LIST_COLUMNS);
    String columnTypeProperty = properties.getProperty(Constants.LIST_COLUMN_TYPES);


    columnNames = Arrays.asList(columnNameProperty.split(","));
    columnTypes = TypeInfoUtils
        .getTypeInfosFromTypeString(columnTypeProperty);
    assert columnNames.size() == columnTypes.size();
    int columns = columnNames.size();

    // Constructing the row ObjectInspector:
    List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(
        columns);
    for (int i = 0; i < columns; i++) {
      columnOIs.add(TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(columnTypes.get(i)));
    }

    // StandardStruct uses ArrayList to store the row.
    rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(
        columnNames, columnOIs);

    // Constructing the row object, etc, which will be reused for all rows.
    row = new ArrayList<Object>(columns);
    for (int c = 0; c < columns; c++) {
      row.add(null);
    }
    outputFields = new Object[columns];
  }

  /**
   * Initializes the internal structures needed
   * for deserializing tuples for a given schema. Typically called once.
   */
  protected void initSchema(Schema schema) throws SerDeException {
    List<Schema.Field> fields = schema.getFields();
    HashMap<String, TypeInfo> columnToType = new HashMap<String, TypeInfo>();
    HashMap<String, Integer> columnToIdx = new HashMap<String, Integer>();

    // Column checking
    for (int i = 0; i < columnNames.size(); i++) {
      String column = columnNames.get(i);
      TypeInfo columnInfo = columnTypes.get(i);

      Schema.Field field = schema.getField(column);
      if (field == null) {
        fail("Column " + column + " not find on input file schema " + schema);
      }

      if (columnInfo.getCategory() != ObjectInspector.Category.PRIMITIVE) {
        fail("Unsuported Hive type for Pangool deserialization. Column " + column + ". " +
            "Hive type " + columnInfo.getTypeName() + ". " + pangoolToHiveSupportedTypes());
      }

      columnToType.put(column, columnInfo);
      columnToIdx.put(column, i);
    }

    // Schema field checking
    rowToTuple = new int[columnNames.size()];
    for (int i = 0; i < fields.size(); i++) {
      Schema.Field field = fields.get(i);

      TypeInfo actualType = columnToType.get(field.getName());
      if (actualType == null) {
        // Field in the schema not present in the table definition. Just ignore it
        continue;
      }

      TypeInfo expectedType = pangoolToHiveConversion.get(field);

      if (!actualType.equals(expectedType)) {
        fail("Uncompatible conversion for column " + field.getName() +
            ". Pangool type: " + field.getType() + ". Hive type: " + actualType.getTypeName()
            + ". " + pangoolToHiveSupportedTypes());
      }

      // rowToTuple indexes conversion
      rowToTuple[columnToIdx.get(field.getName())] = i;
    }

    return;
  }

  /**
   * Builds an schema to be used as output
   */
  protected static Schema buildSchema(String name, List<String> columnNames, List<TypeInfo> columnTypes) throws SerDeException {
    ArrayList<Schema.Field> fields = new ArrayList<Schema.Field>();
    for (int i = 0; i < columnTypes.size(); i++) {
      TypeInfo typeInfo = columnTypes.get(i);
      String column = columnNames.get(i);

      Schema.Field.Type type = hiveToPangoolConversion.get(typeInfo);

      if (type == null) {
        fail("Unsuported column type " + typeInfo.getTypeName() + " for storing column " + column +
            ". " + hiveToPangoolSupportedTypes());
      }
      fields.add(Schema.Field.create(column, type, true));
    }
    Schema schema =  new Schema(name, fields);
    return schema;
  }

  /**
   * Compose a message with the supported types from Pangool to Hive
   */
  private String pangoolToHiveSupportedTypes() {
    String msg = "Supported mappings between Pangool tuple fields and Hive columns: ";
    boolean first = true;
    for (Schema.Field.Type type : pangoolToHiveConversion.keySet()) {
      if (!first) {
        msg += ", ";
      }
      TypeInfo typeInfo = pangoolToHiveConversion.get(type);
      msg += type + " -> " + typeInfo.getTypeName();
      first = false;
    }
    return msg;
  }

  /**
   * Compose a message with the supported types from Hive to Pangool
   */
  private static String hiveToPangoolSupportedTypes() {
    String msg = "Supported mappings between Hive columns to Pangool tuple fields: ";
    boolean first = true;
    for (TypeInfo typeInfo : hiveToPangoolConversion.keySet()) {
      if (!first) {
        msg += ", ";
      }
      Schema.Field.Type type = hiveToPangoolConversion.get(typeInfo);
      msg +=  typeInfo.getTypeName() + " -> " + type ;
      first = false;
    }
    return msg;
  }


  private static void fail(String msg) throws SerDeException {
    throw new SerDeException(msg);
  }

  @Override
  public Object deserialize(Writable writable) throws SerDeException {
    TupleWritable wrapper = (TupleWritable) writable;
    ITuple tuple = wrapper.get();

    if (rowToTuple == null) {
      // First time we see a tuple. We get the Schema, compare with
      // table schema, and initialize the translations.
      initSchema(tuple.getSchema());
    }

    for (int i = 0; i < rowToTuple.length; i++) {
      int tupleId = rowToTuple[i];

      Schema.Field field = tuple.getSchema().getField(tupleId);
      Object value = tuple.get(tupleId);
      if (value == null) {
        row.set(i, null);
      } else {
        switch (field.getType()) {
          case STRING:
            // Hive expects an String
            value = value.toString();
            break;
          case ENUM:
            value = ((Enum) value).ordinal();
            break;
          case BYTES:
            // trying to reuse
            BytesWritable bw = (BytesWritable) row.get(i);
            if (bw == null) {
              bw = new BytesWritable();
            }
            // Hive expect a ByteBuffer
            ByteBuffer bb = (ByteBuffer) value;
            byte[] backedArray = bb.array();
            bw.set(backedArray, 0, bb.limit());
            value = bw;
            break;
        }
        row.set(i, value);
      }
    }

    return row;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return rowOI;
  }

  @Override
  public SerDeStats getSerDeStats() {
    // TODO: implement stats
    return null;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return TupleWritable.class;
  }

  @Override
  public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
    // First time: we create the tuple.
    if (tmpTuple == null) {
      tmpTuple = new Tuple(buildSchema(schemaName, columnNames, columnTypes));
      tupleWrapper = new TupleWritable();
      tupleWrapper.set(tmpTuple);
    }

    StructObjectInspector soi = (StructObjectInspector) objectInspector;

    List<? extends StructField> outputFieldRefs = soi.getAllStructFieldRefs();
    List<Object> structFieldsDataAsList = soi.getStructFieldsDataAsList(o);

    for (int i = 0; i < outputFieldRefs.size(); i++) {
      StructField structFieldRef = outputFieldRefs.get(i);
      Object structFieldData = structFieldsDataAsList.get(i);
      ObjectInspector fieldOI = structFieldRef.getFieldObjectInspector();

      assert fieldOI.getCategory() == ObjectInspector.Category.PRIMITIVE;

      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) fieldOI;
      Object obj;
      poi.getPrimitiveJavaObject(structFieldData);
      switch (poi.getPrimitiveCategory()) {
        case VOID:
          obj = null;
          break;

        default:
          obj = poi.getPrimitiveJavaObject(structFieldData);
      }
      tmpTuple.set(i, obj);
    }

    return tupleWrapper;
  }
}
