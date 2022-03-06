namespace Mondain.Builders

open Abstractions
open System.Collections.Generic
open MongoDB.Driver
open System.Threading.Tasks

[<AutoOpen>]
module Open =
    type CursorEnumerator<'t>(cursor: IAsyncCursor<'t>) =
        let mutable (current: IEnumerator<_>) = Unchecked.defaultof<_>
        interface IAsyncEnumerator<'t> with
            member _.Current = current.Current
            member _.MoveNextAsync() = 
                if (not << isNull) current && current.MoveNext() then
                    ValueTask<_>(result=true)
                else
                    ValueTask<_>(task=task {
                        let! hasNext = cursor.MoveNextAsync()
                        if hasNext then
                            current <- cursor.Current.GetEnumerator()
                            return current.MoveNext()
                        else
                            return hasNext
                    })
            member _.DisposeAsync() = cursor.Dispose() |> ValueTask

    open FindOne
    type MongoQueryBuilder with
        member _.Run(ctx: BaseMongoContext<'T>) =
            task {
                let! cursor = ctx.collection.FindAsync(ctx.filters)
                return! cursor.ToListAsync<'T>()
            }

        member _.Run(ctx: MongoQueryContext<'T>) =
            task {
                let! cursor = ctx.query.collection.FindAsync(ctx.query.filters, ctx.options)
                //cursor.ToListAsync()
                return { new IAsyncEnumerable<'T> with 
                            member _.GetAsyncEnumerator(token) =
                                 CursorEnumerator(cursor) }

            }
    let find = MongoQueryBuilder()
        
    open FindOneAndUpdate
    type MongoFindOneAndUpdateBuilder with
        member _.Run(ctx: MongoFindAndUpdateContext<'T>) =
            let filter = ctx.query.filters
            let update = ctx.update
            let options = ctx.options
            task {
                let! x = ctx.query.collection.FindOneAndUpdateAsync(filter, update, options)
                match box x with
                | null -> return None
                | _ -> return Some x

            }

        member _.Run(ctx: MongoFindAndUpdateContext<'T, 'TProj>) =
            let filter = ctx.query.filters
            let update = ctx.update
            let options = ctx.options
            task {
                //let _ = ctx.query.collection.FindOneAndReplaceAsync
                let! x = ctx.query.collection.FindOneAndUpdateAsync(filter, update, options)
                match box x with
                | null -> return None
                | _ -> return Some x
            }
    let findUpdate = MongoFindOneAndUpdateBuilder()
    //UpdateManyAsync
    //FindOneAndDeleteAsync
    open Update
    type UpdateBuilder with
        member _.Run(ctx: MongoUpdateContext<'T>) =
            ctx.query.collection.UpdateOneAsync(ctx.query.filters, ctx.update, ctx.options)

    let update = UpdateBuilder()
