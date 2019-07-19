/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using System;
using System.Numerics;
using Avro.Generic;

namespace Avro.Util
{
    /// <summary>
    /// The 'decimal' logical type.
    /// </summary>
    public class Decimal : LogicalType
    {
        /// <summary>
        /// The logical type name for Decimal.
        /// </summary>
        public static readonly string LogicalTypeName = "decimal";

        /// <summary>
        /// Initializes a new Decimal logical type.
        /// </summary>
        public Decimal() : base(LogicalTypeName)
        { }

        /// <summary>
        /// Applies 'decimal' logical type validation for a given logical schema.
        /// </summary>
        /// <param name="schema">The schema to be validated.</param>
        public override void ValidateSchema(LogicalSchema schema)
        {
            if (Schema.Type.Bytes != schema.BaseSchema.Tag && Schema.Type.Fixed != schema.BaseSchema.Tag)
                throw new AvroTypeException("'decimal' can only be used with an underlying bytes or fixed type");

            var precisionVal = schema.GetProperty("precision");

            if (string.IsNullOrEmpty(precisionVal))
                throw new AvroTypeException("'decimal' requires a 'precision' property");

            var precision = int.Parse(precisionVal);

            if (precision <= 0)
                throw new AvroTypeException("'decimal' requires a 'precision' property that is greater than zero");

            var scale = GetIntPropertyValueFromSchema(schema, "scale");

            if (scale < 0 || scale > precision)
                throw new AvroTypeException("'decimal' requires a 'scale' property that is zero or less than or equal to 'precision'");
        }

        /// <summary>
        /// Converts a logical value to an instance of its base type.
        /// </summary>
        /// <param name="logicalValue">The logical value to convert.</param>
        /// <param name="schema">The schema that represents the target of the conversion.</param>
        /// <returns>An object representing the encoded value of the base type.</returns>        
        public override object ConvertToBaseValue(object logicalValue, LogicalSchema schema)
        {
            var decimalValue = (AvroDecimal)logicalValue;
            var logicalScale = GetIntPropertyValueFromSchema(schema, "scale");
            var scale = decimalValue.Scale;

            if (scale != logicalScale)
                throw new ArgumentOutOfRangeException("logicalValue", $"The decimal value has a scale of {scale} which cannot be encoded against a logical 'decimal' with a scale of {logicalScale}");

            var buffer = decimalValue.UnscaledValue.ToByteArray();

            Array.Reverse(buffer);

            return Schema.Type.Bytes == schema.BaseSchema.Tag
                ? (object)buffer
                : (object)new GenericFixed(
                    (FixedSchema)schema.BaseSchema,
                    GetDecimalFixedByteArray(buffer, ((FixedSchema)schema.BaseSchema).Size,
                    decimalValue.Sign < 0 ? (byte)0xFF : (byte)0x00));
        }

        private static byte[] GetDecimalFixedByteArray(byte[] sourceBuffer, int size, byte fillValue)
        {
            var paddedBuffer = new byte[size];

            var offset = size - sourceBuffer.Length;

            for (var idx = 0; idx < size; idx++)
            {
                paddedBuffer[idx] = idx < offset ? fillValue : sourceBuffer[idx - offset];
            }

            return paddedBuffer;
        }

        /// <summary>
        /// Converts a base value to an instance of the logical type.
        /// </summary>
        /// <param name="baseValue">The base value to convert.</param>
        /// <param name="schema">The schema that represents the target of the conversion.</param>
        /// <returns>An object representing the encoded value of the logical type.</returns>
        public override object ConvertToLogicalValue(object baseValue, LogicalSchema schema)
        {
            var buffer = Schema.Type.Bytes == schema.BaseSchema.Tag
                ? (byte[])baseValue
                : ((GenericFixed)baseValue).Value;

            Array.Reverse(buffer);

            return new AvroDecimal(new BigInteger(buffer), GetIntPropertyValueFromSchema(schema, "scale"));
        }

        /// <summary>
        /// Retrieve the .NET type that is represented by the logical type implementation.
        /// </summary>
        /// <param name="nullible">A flag indicating whether it should be nullible.</param>
        public override string GetCSharpTypeName(bool nullible)
        {
            var typeName = typeof(AvroDecimal).ToString();
            return nullible ? "System.Nullable<" + typeName + ">" : typeName;
        }

        /// <summary>
        /// Determines if a given object is an instance of the logical Decimal.
        /// </summary>
        /// <param name="logicalValue">The logical value to test.</param>
        public override bool IsInstanceOfLogicalType(object logicalValue)
        {
            return logicalValue is AvroDecimal;
        }

        private static int GetIntPropertyValueFromSchema(Schema schema, string propertyName, int defaultVal = 0)
        {
            var scaleVal = schema.GetProperty(propertyName);

            return string.IsNullOrEmpty(scaleVal) ? defaultVal : int.Parse(scaleVal);
        }
    }
}
