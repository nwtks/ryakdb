namespace RyakDB.Storage

type BlockId = BlockId of fileName: string * number: int64

module BlockId =
    let newBlockId fileName blockNo = BlockId(fileName, blockNo)
