namespace Mondain.Builders

open Abstractions

[<AutoOpen>]
module Open =
    open FindOne
    type MongoQueryBuilder with
        member _.Run(ctx: MongoQueryContext<'T>) =
            ctx.query.collection.FindAsync(ctx.query.filters, ctx.options)

    let find = MongoQueryBuilder()
        
    open FindOneAndUpdate
    type MongoFindOneAndUpdateBuilder with
        member _.Run(ctx: MongoFindAndUpdateContext<'T>) =
            let filter = ctx.query.filters
            let update = ctx.update
            let options = ctx.options
            ctx.query.collection.FindOneAndUpdateAsync(filter, update, options)

        member _.Run(ctx: MongoFindAndUpdateContext<'T, 'TProj>) =
            let filter = ctx.query.filters
            let update = ctx.update
            let options = ctx.options
            ctx.query.collection.FindOneAndUpdateAsync(filter, update, options)

    let findUpdate = MongoFindOneAndUpdateBuilder()

    open Update
    type UpdateBuilder with
        member _.Run(ctx: MongoUpdateContext<'T>) =
            ctx.query.collection.UpdateOneAsync(ctx.query.filters, ctx.update, ctx.options)

    let update = UpdateBuilder()
