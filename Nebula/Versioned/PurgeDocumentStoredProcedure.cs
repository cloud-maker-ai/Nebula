namespace Nebula.Versioned
{
    [StoredProcedure("Nebula.Versioned.PurgeDocument")]
    internal class PurgeDocumentStoredProcedure : StoredProcedureDefinition
    {
        /// <summary>
        /// Purges all document versions with the specified id.
        /// </summary>
        /// <remarks>
        /// <code>
        /// function purgeItem(contentKey, id) {
        ///   var context = getContext();
        ///   var container = context.getCollection();
        /// 
        ///   if (!execute()) {
        ///     throw '[SP_ERROR=READ]';
        ///   }
        /// 
        ///   context.getResponse().setBody(true);
        /// 
        ///   function execute() {
        ///     var query = `SELECT * FROM c WHERE IS_DEFINED(c.${contentKey}) AND c['@documentId'] = '${id}'`;
        /// 
        ///     return container.queryDocuments(
        ///       container.getSelfLink(),
        ///       query,
        ///       deleteItemsCallback
        ///     );
        ///   }
        /// 
        ///   function deleteItem(item) {
        ///     return container.deleteDocument(item._self, {
        ///       'etag': item._etag
        ///     });
        ///   }
        /// 
        ///   function deleteItemsCallback(err, items) {
        ///     if (err) {
        ///       throw '[SP_ERROR=READ]';
        ///     }
        /// 
        ///     items.forEach(item => {
        ///       if (!deleteItem(item)) {
        ///         throw '[SP_ERROR=CONFLICT]';
        ///       }
        ///     });
        ///   }
        /// }
        /// </code>
        /// </remarks>
        private const string ScriptValue = "function purgeItem(e,t){var o,n=getContext(),R=n.getCollection();if(o=`SELECT * FROM c WHERE IS_DEFINED(c.${e}) AND c['@documentId'] = '${t}'`,!R.queryDocuments(R.getSelfLink(),o,E))throw\"[SP_ERROR=READ]\";function E(e,t){if(e)throw\"[SP_ERROR=READ]\";t.forEach(e=>{if(!function(e){return R.deleteDocument(e._self,{etag:e._etag})}(e))throw\"[SP_ERROR=CONFLICT]\"})}n.getResponse().setBody(!0)}";

        public override string Script
        {
            get { return ScriptValue; }
        }
    }
}