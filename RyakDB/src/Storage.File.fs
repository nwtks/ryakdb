namespace RyakDB.Storage.File

open RyakDB.Storage

type FileBuffer = FileBuffer of buffer: byte []

type FileManager =
    { BlockSize: int32
      IsNew: bool
      Size: string -> int64
      Close: string -> unit
      Delete: string -> unit
      Read: FileBuffer -> BlockId -> unit
      Write: FileBuffer -> BlockId -> unit
      Append: FileBuffer -> string -> BlockId }

module FileBuffer =
    let get position size (FileBuffer buffer) =
        Array.init size (fun i -> buffer.[position + i])

    let put position src (FileBuffer buffer) =
        src
        |> Array.iteri (fun i _ -> buffer.[position + i] <- src.[i])

    let clear (FileBuffer buffer) = Array.fill buffer 0 buffer.Length 0uy

    let newFileBuffer capacity = FileBuffer(Array.create capacity 0uy)

module FileManager =
    type FileChannel =
        { Stream: System.IO.Stream
          Lock: System.Threading.ReaderWriterLockSlim }

    module FileChannel =
        let size (channel: FileChannel) =
            channel.Lock.EnterReadLock()
            try
                channel.Stream.Length
            finally
                channel.Lock.ExitReadLock()

        let close (channel: FileChannel) =
            channel.Lock.EnterWriteLock()
            try
                channel.Stream.Dispose()
            finally
                channel.Lock.ExitWriteLock()

        let read position (FileBuffer buffer) (channel: FileChannel) =
            channel.Lock.EnterReadLock()
            try
                channel.Stream.Seek(position, System.IO.SeekOrigin.Begin)
                |> ignore
                channel.Stream.Read(System.Span<byte> buffer)
            finally
                channel.Lock.ExitReadLock()

        let write position (FileBuffer buffer) (channel: FileChannel) =
            channel.Lock.EnterWriteLock()
            try
                channel.Stream.Seek(position, System.IO.SeekOrigin.Begin)
                |> ignore
                channel.Stream.Write(System.ReadOnlySpan<byte> buffer)
            finally
                channel.Lock.ExitWriteLock()

        let append (FileBuffer buffer) (channel: FileChannel) =
            channel.Lock.EnterWriteLock()
            try
                channel.Stream.Seek(0L, System.IO.SeekOrigin.End)
                |> ignore
                channel.Stream.Write(System.ReadOnlySpan<byte> buffer)
            finally
                channel.Lock.ExitWriteLock()

        let newFileChannel fileName =
            { Stream =
                  System.IO.File.Open
                      (fileName,
                       System.IO.FileMode.OpenOrCreate,
                       System.IO.FileAccess.ReadWrite,
                       System.IO.FileShare.ReadWrite)
              Lock = new System.Threading.ReaderWriterLockSlim() }

        let newMemoryChannel () =
            { Stream = new System.IO.MemoryStream()
              Lock = new System.Threading.ReaderWriterLockSlim() }

    let TmpFilePrefix = "_temp"

    let isTempFile (fileName: string) = fileName.StartsWith(TmpFilePrefix)

    type FileManagerState =
        { BlockSize: int64
          DbDirectory: string
          OpenFiles: System.Collections.Concurrent.ConcurrentDictionary<string, FileChannel>
          InMemory: bool
          Anchors: obj [] }

    let private prepareAnchor (anchors: obj []) a =
        let h = hash a % anchors.Length
        if h < 0 then h + anchors.Length else h
        |> anchors.GetValue

    let private getFileChannel state fileName =
        lock (prepareAnchor state.Anchors fileName) (fun () ->
            let newFileChannel name =
                if state.InMemory then
                    FileChannel.newMemoryChannel ()
                else
                    System.IO.Path.Join(state.DbDirectory, name)
                    |> FileChannel.newFileChannel

            state.OpenFiles.GetOrAdd(fileName, newFileChannel))

    let read state buffer (BlockId (fileName, blockNo)) =
        buffer |> FileBuffer.clear
        getFileChannel state fileName
        |> FileChannel.read (blockNo * state.BlockSize) buffer
        |> ignore

    let write state buffer (BlockId (fileName, blockNo)) =
        getFileChannel state fileName
        |> FileChannel.write (blockNo * state.BlockSize) buffer

    let append state buffer fileName =
        let channel = getFileChannel state fileName
        FileChannel.append buffer channel
        BlockId.newBlockId fileName (FileChannel.size channel / state.BlockSize - 1L)

    let size state fileName =
        (getFileChannel state fileName |> FileChannel.size)
        / state.BlockSize

    let close state fileName =
        lock (prepareAnchor state.Anchors fileName) (fun () ->
            let mutable channel = Unchecked.defaultof<FileChannel>
            if state.OpenFiles.TryRemove(fileName, &channel)
            then channel |> FileChannel.close)

    let delete state fileName =
        close state fileName
        (System.IO.Path.Join(state.DbDirectory, fileName)
         |> System.IO.FileInfo).Delete()

    let newFileManager dbPath blockSize inMemory =
        let dbDir, isNew =
            if inMemory then
                dbPath, true
            else
                let di = System.IO.DirectoryInfo(dbPath)
                let dbPathNew = not di.Exists
                if dbPathNew then di.Create()
                di.EnumerateFiles(TmpFilePrefix + "*")
                |> Seq.iter (fun fi -> fi.Delete())
                di.FullName, dbPathNew

        let state =
            { BlockSize = int64 blockSize
              DbDirectory = dbDir
              OpenFiles = System.Collections.Concurrent.ConcurrentDictionary()
              InMemory = inMemory
              Anchors = Array.init 1019 (fun _ -> obj ()) }

        { BlockSize = blockSize
          IsNew = isNew
          Size = size state
          Close = close state
          Delete = delete state
          Read = read state
          Write = write state
          Append = append state }
