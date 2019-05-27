namespace Nebula.Tests
{
    public class TestDocumentDbAccessProvider : IDocumentDbAccessProvider
    {
        private readonly DocumentDbAccess _dbAccess;

        public TestDocumentDbAccessProvider(DocumentDbAccess dbAccess)
        {
            _dbAccess = dbAccess;
        }

        public IDocumentDbAccess GetDbAccess()
        {
            return _dbAccess;
        }
    }
}