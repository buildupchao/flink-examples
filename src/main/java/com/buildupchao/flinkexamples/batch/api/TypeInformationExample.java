package com.buildupchao.flinkexamples.batch.api;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * @see {@link <a>https://segmentfault.com/a/1190000019958294?utm_source=tag-newest</a>}
 * @author buildupchao
 * @date 2020/01/04 22:44
 * @since JDK 1.8
 */
public class TypeInformationExample {

    @TypeInfo(CustomTypeInfoFactory.class)
    public static class CustomTuple<T0, T1> {
        public T0 field0;
        public T1 field1;
    }


    public static class CustomTypeInfoFactory extends TypeInfoFactory<CustomTuple> {

        @Override
        public TypeInformation<CustomTuple> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {

            return new CustomTupleTypeInfo(genericParameters.get("T0"), genericParameters.get("T1"));
        }
    }

    public static class CustomTupleTypeInfo<T0, T1> extends TypeInformation<CustomTuple<T0, T1>> {

        private TypeInformation field0;
        private TypeInformation field1;

        public TypeInformation getField0() {
            return field0;
        }

        public TypeInformation getField1() {
            return field1;
        }

        public CustomTupleTypeInfo(TypeInformation field0, TypeInformation field1) {
            this.field0 = field0;
            this.field1 = field1;
        }

        @Override
        public boolean isBasicType() {
            return false;
        }

        @Override
        public boolean isTupleType() {
            return false;
        }

        @Override
        public int getArity() {
            return 0;
        }

        @Override
        public int getTotalFields() {
            return 0;
        }

        @Override
        public Class<CustomTuple<T0, T1>> getTypeClass() {
            return null;
        }

        @Override
        public boolean isKeyType() {
            return false;
        }

        @Override
        public TypeSerializer<CustomTuple<T0, T1>> createSerializer(ExecutionConfig config) {
            return null;
        }

        @Override
        public String toString() {
            return null;
        }

        @Override
        public boolean equals(Object obj) {
            return false;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public boolean canEqual(Object obj) {
            return false;
        }
    }
}
