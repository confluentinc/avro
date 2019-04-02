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
        }

        /// <summary>
        /// Converts a logical value to an instance of its base type.
        /// </summary>
        /// <param name="logicalValue">The logical value to convert.</param>
        /// <param name="schema">The schema that represents the target of the conversion.</param>
        /// <returns>An object representing the encoded value of the base type.</returns>        
        public override object ConvertToBaseValue(object logicalValue, LogicalSchema schema)
        {
            var val = (decimal)logicalValue;
            var buffer = new byte[16];
            var valBits = decimal.GetBits(val);

            buffer[0] = (byte)valBits[0];
            buffer[1] = (byte)(valBits[0] >> 8);
            buffer[2] = (byte)(valBits[0] >> 16);
            buffer[3] = (byte)(valBits[0] >> 24);

            buffer[4] = (byte)valBits[1];
            buffer[5] = (byte)(valBits[1] >> 8);
            buffer[6] = (byte)(valBits[1] >> 16);
            buffer[7] = (byte)(valBits[1] >> 24);

            buffer[8] = (byte)valBits[2];
            buffer[9] = (byte)(valBits[2] >> 8);
            buffer[10] = (byte)(valBits[2] >> 16);
            buffer[11] = (byte)(valBits[2] >> 24);

            buffer[12] = (byte)valBits[3];
            buffer[13] = (byte)(valBits[3] >> 8);
            buffer[14] = (byte)(valBits[3] >> 16);
            buffer[15] = (byte)(valBits[3] >> 24);

            return Schema.Type.Bytes == schema.BaseSchema.Tag
                ? (object)buffer
                : (object)new GenericFixed((FixedSchema)schema.BaseSchema, buffer);
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
            var valBits = new int[4];

            valBits[0] = buffer[0] | (buffer[1] << 8) | (buffer[2] << 16) | (buffer[3] << 24);
            valBits[1] = buffer[4] | (buffer[5] << 8) | (buffer[6] << 16) | (buffer[7] << 24);
            valBits[2] = buffer[8] | (buffer[9] << 8) | (buffer[10] << 16) | (buffer[11] << 24);
            valBits[3] = buffer[12] | (buffer[13] << 8) | (buffer[14] << 16) | (buffer[15] << 24);

            return new decimal(valBits);
        }

        /// <summary>
        /// Retrieve the .NET type that is represented by the logical type implementation.
        /// </summary>
        /// <param name="nullible">A flag indicating whether it should be nullible.</param>
        public override string GetCSharpTypeName(bool nullible)
        {
            var typeName = typeof(decimal).ToString();
            return nullible ? "System.Nullable<" + typeName + ">" : typeName;
        }

        /// <summary>
        /// Determines if a given object is an instance of the logical Decimal.
        /// </summary>
        /// <param name="logicalValue">The logical value to test.</param>
        public override bool IsInstanceOfLogicalType(object logicalValue)
        {
            return logicalValue is decimal;
        }
    }
}
