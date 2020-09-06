namespace RyakDB.Storage

type BlockId = BlockId of fileName: string * number: int64

module BlockId =
    let newBlockId fileName blockNo = BlockId(fileName, blockNo)

type RecordId = RecordId of id: int32 * blockId: BlockId

module RecordId =
    let newRecordId id blockId = RecordId(id, blockId)

    let newBlockRecordId id fileName blockNo =
        RecordId(id, BlockId.newBlockId fileName blockNo)
