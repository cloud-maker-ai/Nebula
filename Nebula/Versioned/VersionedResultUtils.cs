using System;
using System.Collections.Immutable;
using System.Linq;

namespace Nebula.Versioned
{
    internal static class VersionedResultUtils
    {
        public static VersionedDocumentQueryResult<TTo> ConvertType<TFrom, TTo>(
            VersionedDocumentQueryResult<TFrom> recordResult,
            Func<TFrom, TTo> convertFunc)
        {
            var loaded = recordResult.Loaded.Select(x => ConvertType(x, convertFunc));
            var failed = recordResult.Failed.Select(x => ConvertType(x, convertFunc));

            return new VersionedDocumentQueryResult<TTo>(
                ImmutableList.CreateRange(loaded),
                ImmutableList.CreateRange(failed));
        }

        public static VersionedDocumentReadResult<TTo> ConvertType<TFrom, TTo>(
            VersionedDocumentReadResult<TFrom> recordResult,
            Func<TFrom, TTo> convertFunc)
        {
            if (recordResult.ResultType == DocumentReadResultType.Failed)
            {
                return VersionedDocumentReadResult<TTo>.CreateFailure(
                    recordResult.DocumentId, recordResult.Metadata, recordResult.FailureDetails);
            }

            return VersionedDocumentReadResult<TTo>.CreateOkay(
                recordResult.DocumentId, recordResult.Metadata, convertFunc(recordResult.Document));
        }
    }
}