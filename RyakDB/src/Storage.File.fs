module RyakDB.Storage.File

open RyakDB.Storage

type FileBuffer = FileBuffer of buffer: byte []

type FileManager =
    { BlockSize: int32
      IsNew: bool
      Size: string -> int64
      Close: string -> unit
      CloseAll: unit -> unit
      Delete: string -> unit
      Read: FileBuffer -> BlockId -> unit
      Write: FileBuffer -> BlockId -> unit
      Append: FileBuffer -> string -> BlockId }

module FileBuffer =
    let inline get position size (FileBuffer buffer) =
        Array.init size (fun i -> buffer.[position + i])

    let inline put position src (FileBuffer buffer) =
        src
        |> Array.iteri (fun i _ -> buffer.[position + i] <- src.[i])

    let inline clear (FileBuffer buffer) = Array.fill buffer 0 buffer.Length 0uy

let inline newFileBuffer capacity = FileBuffer(Array.create capacity 0uy)

module FileManager =
    type Channel =
        { Stream: System.IO.Stream
          Lock: System.Threading.ReaderWriterLockSlim }

    module Channel =
        let inline size channel =
            channel.Lock.EnterReadLock()
            try
                channel.Stream.Length
            finally
                channel.Lock.ExitReadLock()

        let inline close channel =
            channel.Lock.EnterWriteLock()
            try
                channel.Stream.Dispose()
            finally
                channel.Lock.ExitWriteLock()

        let inline read position (FileBuffer buffer) channel =
            channel.Lock.EnterReadLock()
            try
                channel.Stream.Seek(position, System.IO.SeekOrigin.Begin)
                |> ignore
                channel.Stream.Read(System.Span<byte> buffer)
            finally
                channel.Lock.ExitReadLock()

        let inline write position (FileBuffer buffer) channel =
            channel.Lock.EnterWriteLock()
            try
                channel.Stream.Seek(position, System.IO.SeekOrigin.Begin)
                |> ignore
                channel.Stream.Write(System.ReadOnlySpan<byte> buffer)
            finally
                channel.Lock.ExitWriteLock()

        let inline append (FileBuffer buffer) channel =
            channel.Lock.EnterWriteLock()
            try
                channel.Stream.Seek(0L, System.IO.SeekOrigin.End)
                |> ignore
                channel.Stream.Write(System.ReadOnlySpan<byte> buffer)
            finally
                channel.Lock.ExitWriteLock()

    let inline newFileChannel fileName =
        { Stream =
              System.IO.File.Open
                  (fileName,
                   System.IO.FileMode.OpenOrCreate,
                   System.IO.FileAccess.ReadWrite,
                   System.IO.FileShare.ReadWrite)
          Lock = new System.Threading.ReaderWriterLockSlim() }

    let inline newMemoryChannel () =
        { Stream = new System.IO.MemoryStream()
          Lock = new System.Threading.ReaderWriterLockSlim() }

    let TmpFilePrefix = "_temp"

    let inline isTempFile (fileName: string) = fileName.StartsWith(TmpFilePrefix)

    type FileManagerState =
        { BlockSize: int64
          DbDirectory: string
          OpenFiles: System.Collections.Concurrent.ConcurrentDictionary<string, Channel>
          InMemory: bool
          Anchors: obj [] }

    let private prepareAnchor (anchors: obj []) a =
        let h = hash a % anchors.Length
        if h < 0 then h + anchors.Length else h
        |> anchors.GetValue

    let private getChannel state fileName =
        lock (prepareAnchor state.Anchors fileName) (fun () ->
            let newFileChannel name =
                if state.InMemory then
                    newMemoryChannel ()
                else
                    System.IO.Path.Join(state.DbDirectory, name)
                    |> newFileChannel

            state.OpenFiles.GetOrAdd(fileName, newFileChannel))

    let read state buffer (BlockId (fileName, blockNo)) =
        buffer |> FileBuffer.clear
        getChannel state fileName
        |> Channel.read (blockNo * state.BlockSize) buffer
        |> ignore

    let write state buffer (BlockId (fileName, blockNo)) =
        getChannel state fileName
        |> Channel.write (blockNo * state.BlockSize) buffer

    let append state buffer fileName =
        let channel = getChannel state fileName
        Channel.append buffer channel
        BlockId.newBlockId fileName (Channel.size channel / state.BlockSize - 1L)

    let size state fileName =
        (getChannel state fileName |> Channel.size)
        / state.BlockSize

    let close state fileName =
        lock (prepareAnchor state.Anchors fileName) (fun () ->
            let mutable channel = Unchecked.defaultof<Channel>
            if state.OpenFiles.TryRemove(fileName, &channel)
            then channel |> Channel.close)

    let closeAll state =
        state.OpenFiles.Keys |> Seq.iter (close state)

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
            di.EnumerateFiles(FileManager.TmpFilePrefix + "*")
            |> Seq.iter (fun fi -> fi.Delete())
            di.FullName, dbPathNew

    let state: FileManager.FileManagerState =
        { BlockSize = int64 blockSize
          DbDirectory = dbDir
          OpenFiles = System.Collections.Concurrent.ConcurrentDictionary()
          InMemory = inMemory
          Anchors = Array.init 1019 (fun _ -> obj ()) }

    { BlockSize = blockSize
      IsNew = isNew
      Size = FileManager.size state
      Close = FileManager.close state
      CloseAll = fun () -> FileManager.closeAll state
      Delete = FileManager.delete state
      Read = FileManager.read state
      Write = FileManager.write state
      Append = FileManager.append state }
