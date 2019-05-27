using System;

namespace Nebula
{
    /// <summary>
    /// A database parameter.
    /// </summary>
    public class DbParameter
    {
        /// <summary>
        /// Initialises a new instance of the <see cref="DbParameter"/> class.
        /// </summary>
        /// <param name="name">The parameter name.</param>
        /// <param name="value">The parameter value.</param>
        public DbParameter(string name, object value)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));
            if (value == null)
                throw new ArgumentNullException(nameof(value));

            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Invalid parameter name", nameof(name));

            Name = name;

            if (Name[0] != '@')
            {
                Name = $"@{name}";
            }

            Value = value;
        }

        /// <summary>
        /// The parameter name.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// The parameter value.
        /// </summary>
        public object Value { get; }
    }
}