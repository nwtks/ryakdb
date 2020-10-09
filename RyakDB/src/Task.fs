module RyakDB.Task

type Task = unit -> unit

type TaskService =
    { RunTask: Task -> unit
      CancelTasks: unit -> unit }

module TaskService =
    let tokenSource =
        new System.Threading.CancellationTokenSource()

    let runTask (task: Task) =
        System.Threading.Tasks.Task.Run(task, tokenSource.Token)
        |> ignore

    let cancelTasks () = tokenSource.Cancel()

let newTaskService () =
    { RunTask = TaskService.runTask
      CancelTasks = TaskService.cancelTasks }
