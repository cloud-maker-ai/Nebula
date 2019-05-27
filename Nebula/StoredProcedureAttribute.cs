using System;

namespace Nebula
{
    internal class StoredProcedureAttribute : Attribute
    {
        public StoredProcedureAttribute(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            Name = name;
        }

        public string Name { get; }
    }
}