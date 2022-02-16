module Mondain.Builders.FindOneAndUpdate

open Abstractions

open MongoDB.Driver
open System
open System.Linq.Expressions

open Mondain.Builders.Core

type MongoFindOneAndUpdateBuilder() =
    inherit BaseMongoBuilder()

    [<CustomOperation("update", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
    member this.Update(
        ctx: BaseMongoContext<'T>, 
        [<ProjectionParameter; ReflectedDefinition>] f: Expression<Func<'T, Update<'a>>>) =
        let struct (updateDef, filters) = createUpdate f 0
        let opts = FindOneAndUpdateOptions<_>(ArrayFilters = filters)
        {| query = ctx; update = updateDef; options = opts |}


    [<CustomOperation("update", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
    member this.Update(
        ctx: MongoFindAndUpdateContext<'T>, 
        [<ProjectionParameter; ReflectedDefinition>] f: Expression<Func<'T, Update<'a>>>) =
        let struct (updateDef, filters) = createUpdate f (ctx.options.ArrayFilters :?> ArrayFilterDefinition[]).Length
        ctx.options.ArrayFilters <- [| yield! ctx.options.ArrayFilters; yield! filters |]
        {| ctx with update = Builders<'T>.Update.Combine(ctx.update, updateDef) |}

    [<CustomOperation("update", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
    member this.Update(
        ctx: BaseMongoContext<'T>, 
        [<ProjectionParameter; ReflectedDefinition>] f: Expression<Func<'T, SeqUpdate<'a>>>) =
        let struct (updateDef, filters) = createUpdateMulti f 0
        let opts = FindOneAndUpdateOptions<_>(ArrayFilters = filters)
        {| query = ctx; update = updateDef; options = opts |}


    [<CustomOperation("update", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
    member this.Update(
        ctx: MongoFindAndUpdateContext<'T>, 
        [<ProjectionParameter; ReflectedDefinition>] f: Expression<Func<'T, SeqUpdate<'a>>>) =
        let struct (updateDef, filters) = createUpdateMulti f (ctx.options.ArrayFilters :?> ArrayFilterDefinition[]).Length
        ctx.options.ArrayFilters <- [| yield! ctx.options.ArrayFilters; yield! filters |]
        {| ctx with update = Builders<'T>.Update.Combine(ctx.update, updateDef) |}

    [<CustomOperation("returnDoc", MaintainsVariableSpace = true)>]
    member _.ReturnDocument(ctx: MongoFindAndUpdateContext<'T> , ret) =
        ctx.options.ReturnDocument <- ret
        ctx

    [<CustomOperation("isUpsert", MaintainsVariableSpace = true)>]
    member _.IsUpsert(ctx: MongoFindAndUpdateContext<'T> , upsert: bool) =
        ctx.options.IsUpsert <- upsert
        ctx

    [<CustomOperation("sort", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
    member _.Sort(ctx: MongoFindAndUpdateContext<'T>, text: string) =
        let sort = Builders<_>.Sort.MetaTextScore(text)            
        match ctx.options.Sort with
        | null -> ctx.options.Sort <- sort
        | s -> ctx.options.Sort <- Builders<_>.Sort.Combine(sort, s)
        ctx

    [<CustomOperation("sortBy", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
    member _.SortBy(ctx: MongoFindAndUpdateContext<'T>, [<ProjectionParameter; ReflectedDefinition>] sort: Expression<_>) =
        let sort = Builders<_>.Sort.Ascending(sort)            
        match ctx.options.Sort with
        | null -> ctx.options.Sort <- sort
        | s -> ctx.options.Sort <- Builders<_>.Sort.Combine(sort, s)
        ctx

    [<CustomOperation("sortByDesc", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
    member _.SortDesc(ctx: MongoFindAndUpdateContext<'T>, [<ProjectionParameter; ReflectedDefinition>] sort: Expression<_>) =
        let sort = Builders<_>.Sort.Descending(sort)            
        match ctx.options.Sort with
        | null -> ctx.options.Sort <- sort
        | s -> ctx.options.Sort <- Builders<_>.Sort.Combine(sort, s)
        ctx

    [<CustomOperation("select")>]
    member _.Projection(
        ctx: MongoFindAndUpdateContext<'T>, 
        [<ProjectionParameter; ReflectedDefinition>] p: Expression<Func<'T, 'TProj>>)
            : MongoFindAndUpdateContext<_,_> =
        let proj = Builders<'T>.Projection.Expression<'TProj>(p)
        //todo copy all options
        let opts = FindOneAndUpdateOptions<_,_>(Projection = proj, Sort = ctx.options.Sort)                
        {| ctx with options = opts |}