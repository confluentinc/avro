using System;
using System.Numerics;

namespace Avro
{
    public struct AvroDecimal : IConvertible, IFormattable, IComparable, IComparable<AvroDecimal>, IEquatable<AvroDecimal>
    {
        public static readonly AvroDecimal MinusOne = new AvroDecimal(BigInteger.MinusOne, 0);
        public static readonly AvroDecimal Zero = new AvroDecimal(BigInteger.Zero, 0);
        public static readonly AvroDecimal One = new AvroDecimal(BigInteger.One, 0);

        private readonly BigInteger _unscaledValue;
        private readonly int _scale;

        public AvroDecimal(double value)
            : this((decimal)value) { }

        public AvroDecimal(float value)
            : this((decimal)value) { }

        public AvroDecimal(decimal value)
        {
            var bytes = FromDecimal(value);

            var unscaledValueBytes = new byte[12];
            Array.Copy(bytes, unscaledValueBytes, unscaledValueBytes.Length);

            var unscaledValue = new BigInteger(unscaledValueBytes);
            var scale = bytes[14];

            if (bytes[15] == 128)
                unscaledValue *= BigInteger.MinusOne;

            _unscaledValue = unscaledValue;
            _scale = scale;
        }

        public AvroDecimal(int value)
            : this(new BigInteger(value), 0) { }

        public AvroDecimal(long value)
            : this(new BigInteger(value), 0) { }

        public AvroDecimal(uint value)
            : this(new BigInteger(value), 0) { }

        public AvroDecimal(ulong value)
            : this(new BigInteger(value), 0) { }

        public AvroDecimal(BigInteger unscaledValue, int scale)
        {
            _unscaledValue = unscaledValue;
            _scale = scale;
        }

        public AvroDecimal(byte[] value)
        {
            byte[] number = new byte[value.Length - 4];
            byte[] flags = new byte[4];

            Array.Copy(value, 0, number, 0, number.Length);
            Array.Copy(value, value.Length - 4, flags, 0, 4);

            _unscaledValue = new BigInteger(number);
            _scale = BitConverter.ToInt32(flags, 0);
        }

        public BigInteger UnscaledValue { get { return _unscaledValue; } }
        public bool IsEven { get { return _unscaledValue.IsEven; } }
        public bool IsOne { get { return _unscaledValue.IsOne; } }
        public bool IsPowerOfTwo { get { return _unscaledValue.IsPowerOfTwo; } }
        public bool IsZero { get { return _unscaledValue.IsZero; } }
        public int Sign { get { return _unscaledValue.Sign; } }
        public int Scale { get { return _scale; } }

        public override string ToString()
        {
            var number = _unscaledValue.ToString("G");

            if (_scale > 0)
                return number.Insert(number.Length - _scale, ".");

            return number;
        }

        public byte[] ToByteArray()
        {
            var unscaledValue = _unscaledValue.ToByteArray();
            var scale = BitConverter.GetBytes(_scale);

            var bytes = new byte[unscaledValue.Length + scale.Length];
            Array.Copy(unscaledValue, 0, bytes, 0, unscaledValue.Length);
            Array.Copy(scale, 0, bytes, unscaledValue.Length, scale.Length);

            return bytes;
        }

        private static byte[] FromDecimal(decimal d)
        {
            byte[] bytes = new byte[16];

            int[] bits = decimal.GetBits(d);
            int lo = bits[0];
            int mid = bits[1];
            int hi = bits[2];
            int flags = bits[3];

            bytes[0] = (byte)lo;
            bytes[1] = (byte)(lo >> 8);
            bytes[2] = (byte)(lo >> 0x10);
            bytes[3] = (byte)(lo >> 0x18);
            bytes[4] = (byte)mid;
            bytes[5] = (byte)(mid >> 8);
            bytes[6] = (byte)(mid >> 0x10);
            bytes[7] = (byte)(mid >> 0x18);
            bytes[8] = (byte)hi;
            bytes[9] = (byte)(hi >> 8);
            bytes[10] = (byte)(hi >> 0x10);
            bytes[11] = (byte)(hi >> 0x18);
            bytes[12] = (byte)flags;
            bytes[13] = (byte)(flags >> 8);
            bytes[14] = (byte)(flags >> 0x10);
            bytes[15] = (byte)(flags >> 0x18);

            return bytes;
        }

        #region Operators

        public static bool operator ==(AvroDecimal left, AvroDecimal right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(AvroDecimal left, AvroDecimal right)
        {
            return !left.Equals(right);
        }

        public static bool operator >(AvroDecimal left, AvroDecimal right)
        {
            return (left.CompareTo(right) > 0);
        }

        public static bool operator >=(AvroDecimal left, AvroDecimal right)
        {
            return (left.CompareTo(right) >= 0);
        }

        public static bool operator <(AvroDecimal left, AvroDecimal right)
        {
            return (left.CompareTo(right) < 0);
        }

        public static bool operator <=(AvroDecimal left, AvroDecimal right)
        {
            return (left.CompareTo(right) <= 0);
        }

        public static bool operator ==(AvroDecimal left, decimal right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(AvroDecimal left, decimal right)
        {
            return !left.Equals(right);
        }

        public static bool operator >(AvroDecimal left, decimal right)
        {
            return (left.CompareTo(right) > 0);
        }

        public static bool operator >=(AvroDecimal left, decimal right)
        {
            return (left.CompareTo(right) >= 0);
        }

        public static bool operator <(AvroDecimal left, decimal right)
        {
            return (left.CompareTo(right) < 0);
        }

        public static bool operator <=(AvroDecimal left, decimal right)
        {
            return (left.CompareTo(right) <= 0);
        }

        public static bool operator ==(decimal left, AvroDecimal right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(decimal left, AvroDecimal right)
        {
            return !left.Equals(right);
        }

        public static bool operator >(decimal left, AvroDecimal right)
        {
            return (left.CompareTo(right) > 0);
        }

        public static bool operator >=(decimal left, AvroDecimal right)
        {
            return (left.CompareTo(right) >= 0);
        }

        public static bool operator <(decimal left, AvroDecimal right)
        {
            return (left.CompareTo(right) < 0);
        }

        public static bool operator <=(decimal left, AvroDecimal right)
        {
            return (left.CompareTo(right) <= 0);
        }

        #endregion

        #region Explicity and Implicit Casts

        public static explicit operator byte(AvroDecimal value) { return value.ToType<byte>(); }
        public static explicit operator sbyte(AvroDecimal value) { return value.ToType<sbyte>(); }
        public static explicit operator short(AvroDecimal value) { return value.ToType<short>(); }
        public static explicit operator int(AvroDecimal value) { return value.ToType<int>(); }
        public static explicit operator long(AvroDecimal value) { return value.ToType<long>(); }
        public static explicit operator ushort(AvroDecimal value) { return value.ToType<ushort>(); }
        public static explicit operator uint(AvroDecimal value) { return value.ToType<uint>(); }
        public static explicit operator ulong(AvroDecimal value) { return value.ToType<ulong>(); }
        public static explicit operator float(AvroDecimal value) { return value.ToType<float>(); }
        public static explicit operator double(AvroDecimal value) { return value.ToType<double>(); }
        public static explicit operator decimal(AvroDecimal value) { return value.ToType<decimal>(); }
        public static explicit operator BigInteger(AvroDecimal value)
        {
            var scaleDivisor = BigInteger.Pow(new BigInteger(10), value._scale);
            var scaledValue = BigInteger.Divide(value._unscaledValue, scaleDivisor);
            return scaledValue;
        }

        public static implicit operator AvroDecimal(byte value) { return new AvroDecimal(value); }
        public static implicit operator AvroDecimal(sbyte value) { return new AvroDecimal(value); }
        public static implicit operator AvroDecimal(short value) { return new AvroDecimal(value); }
        public static implicit operator AvroDecimal(int value) { return new AvroDecimal(value); }
        public static implicit operator AvroDecimal(long value) { return new AvroDecimal(value); }
        public static implicit operator AvroDecimal(ushort value) { return new AvroDecimal(value); }
        public static implicit operator AvroDecimal(uint value) { return new AvroDecimal(value); }
        public static implicit operator AvroDecimal(ulong value) { return new AvroDecimal(value); }
        public static implicit operator AvroDecimal(float value) { return new AvroDecimal(value); }
        public static implicit operator AvroDecimal(double value) { return new AvroDecimal(value); }
        public static implicit operator AvroDecimal(decimal value) { return new AvroDecimal(value); }
        public static implicit operator AvroDecimal(BigInteger value) { return new AvroDecimal(value, 0); }

        #endregion

        public T ToType<T>() where T : struct
        {
            return (T)((IConvertible)this).ToType(typeof(T), null);
        }

        object IConvertible.ToType(Type conversionType, IFormatProvider provider)
        {
            var scaleDivisor = BigInteger.Pow(new BigInteger(10), _scale);
            var remainder = BigInteger.Remainder(_unscaledValue, scaleDivisor);
            var scaledValue = BigInteger.Divide(_unscaledValue, scaleDivisor);

            if (scaledValue > new BigInteger(Decimal.MaxValue))
                throw new ArgumentOutOfRangeException("value", "The value " + _unscaledValue + " cannot fit into " + conversionType.Name + ".");

            var leftOfDecimal = (decimal)scaledValue;
            var rightOfDecimal = ((decimal)remainder) / ((decimal)scaleDivisor);

            var value = leftOfDecimal + rightOfDecimal;
            return Convert.ChangeType(value, conversionType);
        }

        public override bool Equals(object obj)
        {
            return ((obj is AvroDecimal) && Equals((AvroDecimal)obj));
        }

        public override int GetHashCode()
        {
            return _unscaledValue.GetHashCode() ^ _scale.GetHashCode();
        }

        #region IConvertible Members

        TypeCode IConvertible.GetTypeCode()
        {
            return TypeCode.Object;
        }

        bool IConvertible.ToBoolean(IFormatProvider provider)
        {
            return Convert.ToBoolean(this);
        }

        byte IConvertible.ToByte(IFormatProvider provider)
        {
            return Convert.ToByte(this);
        }

        char IConvertible.ToChar(IFormatProvider provider)
        {
            throw new InvalidCastException("Cannot cast BigDecimal to Char");
        }

        DateTime IConvertible.ToDateTime(IFormatProvider provider)
        {
            throw new InvalidCastException("Cannot cast BigDecimal to DateTime");
        }

        decimal IConvertible.ToDecimal(IFormatProvider provider)
        {
            return Convert.ToDecimal(this);
        }

        double IConvertible.ToDouble(IFormatProvider provider)
        {
            return Convert.ToDouble(this);
        }

        short IConvertible.ToInt16(IFormatProvider provider)
        {
            return Convert.ToInt16(this);
        }

        int IConvertible.ToInt32(IFormatProvider provider)
        {
            return Convert.ToInt32(this);
        }

        long IConvertible.ToInt64(IFormatProvider provider)
        {
            return Convert.ToInt64(this);
        }

        sbyte IConvertible.ToSByte(IFormatProvider provider)
        {
            return Convert.ToSByte(this);
        }

        float IConvertible.ToSingle(IFormatProvider provider)
        {
            return Convert.ToSingle(this);
        }

        string IConvertible.ToString(IFormatProvider provider)
        {
            return Convert.ToString(this);
        }

        ushort IConvertible.ToUInt16(IFormatProvider provider)
        {
            return Convert.ToUInt16(this);
        }

        uint IConvertible.ToUInt32(IFormatProvider provider)
        {
            return Convert.ToUInt32(this);
        }

        ulong IConvertible.ToUInt64(IFormatProvider provider)
        {
            return Convert.ToUInt64(this);
        }

        #endregion

        #region IFormattable Members

        public string ToString(string format, IFormatProvider formatProvider)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region IComparable Members

        public int CompareTo(object obj)
        {
            if (obj == null)
                return 1;

            if (!(obj is AvroDecimal))
                throw new ArgumentException("Compare to object must be a BigDecimal", "obj");

            return CompareTo((AvroDecimal)obj);
        }

        #endregion

        #region IComparable<BigDecimal> Members

        public int CompareTo(AvroDecimal other)
        {
            var unscaledValueCompare = _unscaledValue.CompareTo(other._unscaledValue);
            var scaleCompare = _scale.CompareTo(other._scale);

            // if both are the same value, return the value
            if (unscaledValueCompare == scaleCompare)
                return unscaledValueCompare;

            // if the scales are both the same return unscaled value
            if (scaleCompare == 0)
                return unscaledValueCompare;

            var scaledValue = BigInteger.Divide(_unscaledValue, BigInteger.Pow(new BigInteger(10), _scale));
            var otherScaledValue = BigInteger.Divide(other._unscaledValue, BigInteger.Pow(new BigInteger(10), other._scale));

            return scaledValue.CompareTo(otherScaledValue);
        }

        #endregion

        #region IEquatable<BigDecimal> Members

        public bool Equals(AvroDecimal other)
        {
            return _scale == other._scale && _unscaledValue == other._unscaledValue;
        }

        #endregion
    }
}
