module Mondain.Builders.FindOne

open Abstractions

open MongoDB.Driver
open System
open System.Linq.Expressions

type MongoQueryBuilder() =
    inherit BaseMongoBuilder()

    [<CustomOperation("limit", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
    member _.Limit(ctx: BaseMongoContext<'T>, limit) =
        {| query = ctx; options = (FindOptions<_>(Limit = limit)) |}

    [<CustomOperation("limit", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
    member _.Limit(ctx: MongoQueryContext<'T>, limit) : MongoQueryContext<'T> =
        ctx.options.Limit <- limit
        ctx

    [<CustomOperation("limit", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
    member _.Limit(ctx: MongoQueryContext<'T, 'TProj>, limit) : MongoQueryContext<'T, 'TProj> =
        ctx.options.Limit <- limit
        ctx

    [<CustomOperation("skip", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
    member _.Skip(ctx: MongoQueryContext<'T>, skip) =
        ctx.options.Skip <- skip
        ctx

    [<CustomOperation("sort", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
    member _.Sort(ctx: MongoQueryContext<'T>, text: string) =
        let sort = Builders<_>.Sort.MetaTextScore(text)            
        match ctx.options.Sort with
        | null -> ctx.options.Sort <- sort
        | s -> ctx.options.Sort <- Builders<_>.Sort.Combine(sort, s)
        ctx

    [<CustomOperation("sortBy", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
    member _.SortBy(ctx: MongoQueryContext<'T>, [<ProjectionParameter; ReflectedDefinition>] sort: Expression<_>) =
        let sort = Builders<_>.Sort.Ascending(sort)            
        match ctx.options.Sort with
        | null -> ctx.options.Sort <- sort
        | s -> ctx.options.Sort <- Builders<_>.Sort.Combine(sort, s)
        ctx

    [<CustomOperation("sortByDesc", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
    member _.SortDesc(ctx: MongoQueryContext<'T>, [<ProjectionParameter; ReflectedDefinition>] sort: Expression<_>) =
        let sort = Builders<_>.Sort.Descending(sort)            
        match ctx.options.Sort with
        | null -> ctx.options.Sort <- sort
        | s -> ctx.options.Sort <- Builders<_>.Sort.Combine(sort, s)
        ctx

    [<CustomOperation("select")>]
    member _.Projection(
        ctx: BaseMongoContext<'T>, 
        [<ProjectionParameter; ReflectedDefinition>] p: Expression<Func<'T, 'TProj>>)
            : MongoQueryContext<_,_> =
        let proj = Builders<'T>.Projection.Expression<'TProj>(p)
        let opts = FindOptions<_,_>(Projection = proj)                
        {| query = ctx; options = opts |}
            
    [<CustomOperation("select")>]
    member _.Projection(
        ctx: MongoQueryContext<'T>, 
        [<ProjectionParameter; ReflectedDefinition>] p: Expression<Func<'T, 'TProj>>)
            : MongoQueryContext<_,_> =
        let proj = Builders<'T>.Projection.Expression<'TProj>(p)
        let opts = FindOptions<_,_>(Projection = proj, Limit = ctx.options.Limit, Skip = ctx.options.Skip, Sort = ctx.options.Sort)                
        {| ctx with options = opts |}

    [<CustomOperation("with_options")>]
    member _.WithOptions(ctx: BaseMongoContext<'T>, fOpts) =    
        let opts = FindOptions<_>()
        do fOpts (opts)
        {| query = ctx; options = opts |}

    [<CustomOperation("with_options")>]
    member _.WithOptions(ctx: MongoQueryContext<'T>, fOpts) = 
        do fOpts (ctx.options)
        ctx

    [<CustomOperation("with_options")>]
    member _.WithOptions(ctx: MongoQueryContext<'T, 'TProj>, fOpts) = 
        do fOpts (ctx.options)
        ctx




