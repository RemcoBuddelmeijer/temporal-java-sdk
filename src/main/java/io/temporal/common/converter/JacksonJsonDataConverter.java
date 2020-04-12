/*
 *  Copyright (C) 2020 Temporal Technologies, Inc. All Rights Reserved.
 *
 *  Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Modifications copyright (C) 2017 Uber Technologies, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"). You may not
 *  use this file except in compliance with the License. A copy of the License is
 *  located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package io.temporal.common.converter;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES;
import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_SINGLE_ELEM_ARRAYS_UNWRAPPED;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.base.Defaults;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Implements conversion through Jackson JSON processor.
 *
 * <p>Replacement for {@link GsonJsonDataConverter} and new default for {@link
 * io.temporal.client.WorkflowClientOptions}.
 *
 * @author remcobuddelmeijer
 */
public final class JacksonJsonDataConverter implements DataConverter {

  private static final DataConverter INSTANCE = new JacksonJsonDataConverter();

  private static final SerializationFeature[] DEFAULT_SERIALIZATION_FEATURES =
      new SerializationFeature[]{WRITE_SINGLE_ELEM_ARRAYS_UNWRAPPED};

  private static final DeserializationFeature[] DEFAULT_DESERIALIZATION_FEATURES =
      new DeserializationFeature[]{FAIL_ON_NULL_FOR_PRIMITIVES};

  private final ObjectMapper mapper;

  public static DataConverter getInstance() {
    return INSTANCE;
  }

  private JacksonJsonDataConverter() {
    this(ObjectMapper::new);
  }

  public JacksonJsonDataConverter(Supplier<ObjectMapper> mapperSupplier) {
    this.mapper = mapperSupplier.get();
  }

  @Override
  public byte[] toData(Object... values) throws DataConverterException {
    if (values == null || values.length == 0) {
      return null;
    }

    try {
      return constructWriter(mapper.writer()).writeValueAsBytes(values);
    } catch (IOException e) {
      throw new DataConverterException("Unable to read value from values", e.getCause());
    }
  }

  @Override
  public <T> T fromData(byte[] content, Class<T> valueClass, Type valueType)
      throws DataConverterException {
    if (content == null) {
      return null;
    }

    try {
      return constructReaderForType(mapper.reader(), valueClass).readValue(content);
    } catch (IOException e) {
      throw new DataConverterException(content, new Type[]{valueType}, e);
    }
  }

  @Override
  public Object[] fromDataArray(byte[] content, Type... valueTypes) throws DataConverterException {
    if (content == null) {
      if (valueTypes.length == 0) {
        return new Object[0];
      }
      throw new DataConverterException(
          "Content doesn't match expected arguments", null, valueTypes);
    }

    // Root Object reader, will be inherited from
    final ObjectReader reader = mapper.reader();

    if (valueTypes.length == 1) {
      try {
        return new Object[]{constructReaderForType(reader, valueTypes[0]).readValue(content)};
      } catch (IOException e) {
        throw new DataConverterException(content, valueTypes, e);
      }
    }
    JsonNode node;
    try {
      node = reader.readTree(content);
    } catch (IOException e) {
      throw new DataConverterException("Unable to create JsonNode", content, valueTypes);
    }

    ArrayNode array;
    if (node.isArray()) {
      array = (ArrayNode) node;
    } else {
      array = (ArrayNode) reader.createArrayNode();
      array.add(node);
    }

    Object[] result = new Object[valueTypes.length];

    for (int i = 0; i < result.length; i++) {
      if (i >= array.size()) {
        Type t = valueTypes[i];
        if (t instanceof Class) {
          result[i] = Defaults.defaultValue(TypeFactory.rawClass(t));
        } else {
          result[i] = null;
        }
      } else {
        try {
          result[i] = constructReaderForType(reader, valueTypes[i]).readValue(content);
        } catch (IOException e) {
          Type[] failingType = new Type[1];
          failingType[0] = valueTypes[i];
          throw new DataConverterException(content, failingType, e);
        }
      }
    }

    return result;
  }

  private ObjectReader constructReaderForType(
      ObjectReader root, Type type, DeserializationFeature... features) {
    TypeFactory factory = root.getTypeFactory();
    JavaType javaType = factory.constructType(type);
    return root.forType(javaType)
        .withFeatures(
            Stream.concat(Arrays.stream(features), Arrays.stream(DEFAULT_DESERIALIZATION_FEATURES))
                .toArray(DeserializationFeature[]::new));
  }

  private ObjectWriter constructWriter(ObjectWriter root, SerializationFeature... features) {
    return root.withFeatures(
        Stream.concat(Arrays.stream(features), Arrays.stream(DEFAULT_SERIALIZATION_FEATURES))
            .toArray(SerializationFeature[]::new));
  }
}
