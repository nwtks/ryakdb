namespace RyakDB.Storage

type BlockId = BlockId of fileName: string * blockNo: int64

module BlockId =
    let BlockNoSize = 8

    let inline fileName (BlockId (fileName, _)) = fileName

    let inline blockNo (BlockId (_, blockNo)) = blockNo

    let inline newBlockId fileName blockNo = BlockId(fileName, blockNo)
