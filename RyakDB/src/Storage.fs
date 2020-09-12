namespace RyakDB.Storage

type BlockId = BlockId of fileName: string * blockNo: int64

module BlockId =
    let BlockNoSize = 8

    let inline newBlockId fileName blockNo = BlockId(fileName, blockNo)
