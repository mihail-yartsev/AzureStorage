using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Lykke.AzureStorage.Tables
{
    public class AzureTableEntity: ITableEntity
    {
        private static readonly ConcurrentDictionary<Type, TypeInfo> _typesInfoCache = new ConcurrentDictionary<Type, TypeInfo>();

        string ITableEntity.PartitionKey { get; set; }
        string ITableEntity.RowKey { get; set; }
        DateTimeOffset ITableEntity.Timestamp { get; set; }
        string ITableEntity.ETag { get; set; }

        void ITableEntity.ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            ReadEntity(properties, operationContext, GetCurrentTypeInfoCached());
        }

        protected virtual void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext, TypeInfo typeInfo)
        {
            foreach (var prop in properties)
            {
                if (typeInfo.Properties.TryGetValue(prop.Key, out var propInfo))
                {
                    propInfo.Setter(this, prop.Value);
                }
            }

            typeInfo.PartitionKeyProp.Setter(this, ((ITableEntity)this).PartitionKey);
            typeInfo.RowKeyProp.Setter(this, ((ITableEntity)this).RowKey);
        }

        IDictionary<string, EntityProperty> ITableEntity.WriteEntity(OperationContext operationContext)
        {
            return WriteEntity(operationContext, GetCurrentTypeInfoCached());
        }

        protected virtual IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext, TypeInfo typeInfo)
        {
            var result = typeInfo.Properties.ToDictionary(p => p.Key, p => p.Value.Getter(this));
            ((ITableEntity) this).PartitionKey = typeInfo.PartitionKeyProp.Getter(this);
            ((ITableEntity) this).RowKey = typeInfo.RowKeyProp.Getter(this);
            return result;
        }

        private TypeInfo GetCurrentTypeInfoCached()
        {
            return _typesInfoCache.GetOrAdd(GetType(), GenerateTypeInfo);
        }

        private static TypeInfo GenerateTypeInfo(Type type)
        {
            var props = new Dictionary<string, PropInfo>(StringComparer.OrdinalIgnoreCase);
            KeyPropInfo partitionKeyProp = null;
            KeyPropInfo rowKeyProp = null;

            var constPartitionKey = type.GetCustomAttribute<ConstPartitionKeyAttribute>();
            if (constPartitionKey != null)
            {
                if (string.IsNullOrWhiteSpace(constPartitionKey.Value))
                {
                    throw new InvalidOperationException("ConstPartitionKey should not be empty");
                }

                partitionKeyProp = new KeyPropInfo(o => constPartitionKey.Value, (o, p) => { });
            }

            foreach (var p in type.GetProperties())
            {
                if (p.GetCustomAttribute<IgnorePropertyAttribute>() != null)
                {
                    continue;
                }

                if (p.GetCustomAttribute<PartitionKeyAttribute>() != null)
                {
                    if (constPartitionKey != null)
                    {
                        throw new InvalidOperationException(
                            "Entity cannot have a PartitionKey attribute specified along with the ConstPartitionKey attribute");
                    }

                    if (partitionKeyProp != null)
                    {
                        throw new InvalidOperationException(
                            "Entity cannot have a PartitionKey attribute specified twice");
                    }

                    partitionKeyProp = new KeyPropInfo(MakeKeyGetter(type, p), MakeKeySetter(type, p));
                    continue;
                }

                if (p.GetCustomAttribute<RowKeyAttribute>() != null)
                {
                    if (rowKeyProp != null)
                    {
                        throw new InvalidOperationException(
                            "Entity cannot have a RowKey attribute specified twice");
                    }

                    rowKeyProp = new KeyPropInfo(MakeKeyGetter(type, p), MakeKeySetter(type, p));
                    continue;
                }

                props[p.Name] = new PropInfo(MakePropGetter(type, p), MakePropSetter(type, p));
            }

            if (partitionKeyProp == null)
            {
                throw new InvalidOperationException("Entity should have a partition key specified");
            }

            if (rowKeyProp == null)
            {
                throw new InvalidOperationException("Entity should have a row key specified");
            }

            return new TypeInfo(props, partitionKeyProp, rowKeyProp);
        }

        private static Func<object, string> MakeKeyGetter(Type type, PropertyInfo property)
        {
            if (!property.CanRead)
            {
                throw new InvalidOperationException($"A RowKey property \"{type.FullName}.{property.Name}\" should be readable");
            }

            ValidatePropertyNotNullable(type, property);
            ValidatePropertyTypeSupported(type, property, property.PropertyType);

            var getter = MakeGetterLambda(type, property);
            return o => string.Format(CultureInfo.InvariantCulture, "{0}", getter(o));
        }

        private static Action<object, string> MakeKeySetter(Type type, PropertyInfo property)
        {
            if (!property.CanWrite)
            {
                throw new InvalidOperationException($"A RowKey property \"{type.FullName}.{property.Name}\" should be writable");
            }

            ValidatePropertyNotNullable(type, property);
            ValidatePropertyTypeSupported(type, property, property.PropertyType);

            var setter = MakeSetterLambda(type, property);
            return (o, str) => setter(o, Convert.ChangeType(str, property.PropertyType, CultureInfo.InvariantCulture));
        }

        private static void ValidatePropertyNotNullable(Type type, PropertyInfo property)
        {
            if (Nullable.GetUnderlyingType(property.PropertyType) != null)
            {
                throw new InvalidOperationException(
                    $"Property \"{type.FullName}.{property.Name}\" is of type \"{property.PropertyType}\". Nullable types are not supported for entity keys");
            }
        }

        private static void ValidatePropertyTypeSupported(Type type, PropertyInfo property, Type propertyType)
        {
            if (propertyType != typeof(string) && propertyType != typeof(byte[]) && propertyType != typeof(decimal) &&
                EntityProperty.CreateEntityPropertyFromObject(Activator.CreateInstance(propertyType)) == null)
            {
                throw new InvalidOperationException(
                    $"Property \"{type.FullName}.{property.Name}\" is of type \"{property.PropertyType.FullName}\" which is not supported");
            }
        }

        private static Func<object, EntityProperty> MakePropGetter(Type type, PropertyInfo property)
        {
            if (!property.CanRead)
            {
                return null;
            }

            var getter = MakeGetterLambda(type, property);

            var underlyingType = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;
            var isPropDecimal = underlyingType == typeof(decimal);
            ValidatePropertyTypeSupported(type, property, underlyingType);

            return o =>
            {
                var value = getter(o);
                if (isPropDecimal && value != null)
                {
                    value = (double)(decimal)value;
                }

                // todo: pull out the inner switch on type, so that it'll be exetuted on lambda creation only
                return EntityProperty.CreateEntityPropertyFromObject(value);
            };
        }

        private static Action<object, EntityProperty> MakePropSetter(Type type, PropertyInfo property)
        {
            if (!property.CanWrite)
            {
                return null;
            }

            var setter = MakeSetterLambda(type, property);
            var underlyingType = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;
            var isPropDecimal = underlyingType == typeof(decimal);
            ValidatePropertyTypeSupported(type, property, underlyingType);

            return (o, e) =>
            {
                var value = e.PropertyAsObject;
                if (isPropDecimal && value is double d)
                {
                    value = (decimal) d;
                }

                setter(o, value);
            };
        }

        private static Func<object, object> MakeGetterLambda(Type type, PropertyInfo property)
        {
            var param = Expression.Parameter(typeof(object), "param");
            Expression propExpression =
                Expression.Convert(Expression.Property(Expression.Convert(param, type), property), typeof(object));
            var lambda = Expression.Lambda(propExpression, param);
            var getter = (Func<object, object>) lambda.Compile();
            return getter;
        }

        private static Action<object, object> MakeSetterLambda(Type type, PropertyInfo property)
        {
            var thisParam = Expression.Parameter(typeof(object), "this");
            var theValue = Expression.Parameter(typeof(object), "value");
            var isValueType = !property.PropertyType.IsClass && !property.PropertyType.IsInterface;

            Expression valueExpression = isValueType
                ? Expression.Unbox(theValue, property.PropertyType)
                : Expression.Convert(theValue, property.PropertyType);

            var thisExpression = Expression.Property(Expression.Convert(thisParam, type), property);
            Expression body = Expression.Assign(thisExpression, valueExpression);

            var block = Expression.Block(new[] {body, Expression.Empty()});

            var lambda = Expression.Lambda(block, thisParam, theValue);
            return (Action<object, object>) lambda.Compile();
        }

        protected class PropInfo
        {
            public Func<object, EntityProperty> Getter { get; }
            public Action<object, EntityProperty> Setter { get; }

            public PropInfo(Func<object, EntityProperty> getter, Action<object, EntityProperty> setter)
            {
                Getter = getter;
                Setter = setter;
            }
        }

        protected class KeyPropInfo
        {
            public Func<object, string> Getter { get; }
            public Action<object, string> Setter { get; }

            public KeyPropInfo(Func<object, string> getter, Action<object, string> setter)
            {
                Getter = getter;
                Setter = setter;
            }
        }

        protected class TypeInfo
        {
            public IReadOnlyDictionary<string, PropInfo> Properties { get; }
            public KeyPropInfo PartitionKeyProp { get; }
            public KeyPropInfo RowKeyProp { get; }

            public TypeInfo(IReadOnlyDictionary<string, PropInfo> properties, KeyPropInfo partitionKeyProp, KeyPropInfo rowKeyProp)
            {
                Properties = properties;
                PartitionKeyProp = partitionKeyProp;
                RowKeyProp = rowKeyProp;
            }
        }

        [AttributeUsage(AttributeTargets.Property)]
        protected class PartitionKeyAttribute : Attribute
        {
        }

        [AttributeUsage(AttributeTargets.Class)]
        protected class ConstPartitionKeyAttribute : Attribute
        {
            public string Value { get; }

            public ConstPartitionKeyAttribute(string value)
            {
                Value = value;
            }
        }

        [AttributeUsage(AttributeTargets.Property)]
        protected class RowKeyAttribute : Attribute
        {
        }
    }
}
