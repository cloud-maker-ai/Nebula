namespace Nebula.Versioned
{
    [StoredProcedure("Nebula.Versioned.CreateDocument")]
    internal class CreateDocumentStoredProcedure : StoredProcedureDefinition
    {
        /// <summary>
        /// Stores a document and updates the '@latest' flag.
        /// </summary>
        /// <remarks>
        /// <para>The existing item has the '@latest. flag cleared. The new item has the '@latest' flag set.</para>
        /// <code>
        /// function storeItem(newItem, existingItem) {
        /// 
        ///   var context = getContext();
        ///   var container = context.getCollection();
        /// 
        ///   if (existingItem) {
        ///     if (!replaceItem(existingItem)) {
        ///       throw '[SP_ERROR=CONFLICT]';
        ///     }
        ///   }
        /// 
        ///   if (!createItem(newItem)) {
        ///     throw '[SP_ERROR=CONFLICT]';
        ///   }
        /// 
        ///   context.getResponse().setBody(newItem.id)
        /// 
        ///   function createItem(item) {
        ///     item['@latest'] = true;
        ///     return container.createDocument(container.getSelfLink(), item);
        ///   }
        /// 
        ///   function replaceItem(item) {
        ///     delete item['@latest'];
        ///     return container.replaceDocument(item._self, item);
        ///   }
        /// }
        /// </code>
        /// </remarks>
        private const string ScriptValue = "function storeItem(e,t){var o,l,n=getContext(),s=n.getCollection();if(t&&(delete(o=t)[\"@latest\"],!s.replaceDocument(o._self,o)))throw\"[SP_ERROR=CONFLICT]\";if((l=e)[\"@latest\"]=!0,!s.createDocument(s.getSelfLink(),l))throw\"[SP_ERROR=CONFLICT]\";n.getResponse().setBody(e.id)}";

        public override string Script
        {
            get { return ScriptValue; }
        }
    }
}