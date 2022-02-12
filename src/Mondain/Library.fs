namespace Mondain
open MongoDB.Driver
open MongoDB.Bson
open System
open System.Linq.Expressions

type BaseMongoContext<'a> = 
    {
        collection: IMongoCollection<'a>
        filters: FilterDefinition<'a>
    }
type MongoQueryContext<'a> =
    {|
        query: BaseMongoContext<'a>
        options: FindOptions<'a>
    |}

type MongoQueryContext<'a, 'b> =
    {|
        query: BaseMongoContext<'a>
        options: FindOptions<'a, 'b>
    |}

type MongoUpdateContext<'a> =
    {
        query: BaseMongoContext<'a>
        update: UpdateDefinition<'a>
    }

module Builder =
    [<Struct;NoComparison;NoEquality>]
    type SetUpdate<'a> = private W of 'a
    [<Struct;NoComparison;NoEquality>]
    type IncrUpdate<'a> = private I of 'a

    [<AutoOpen>]
    module Operators =
        let (:=) (a: 'a) (_: 'a) = W a

        let (+=) (a: 'a) (_: 'a) = I a

    type MongoExpressionParser =
        static member ToExpression(e: Expression<Func<'T, SetUpdate<'a>>>) : struct ('a * Expression<Func<'T, 'a>>) =
            failwith ""

        static member ToExpression(e: Expression<Func<'T, IncrUpdate<'a>>>) : struct ('a * Expression<Func<'T, 'a>>) =
            failwith ""


    //let query (collection: IMongoCollection<_>) =
    //    collection.
    [<AbstractClass>]
    type BaseMongoBuilder() =
        member _.Yield(x: 'T) = ()

        member _.For(s: IMongoCollection<'T>, _: 'T -> unit) =
            s
        [<CustomOperation("where", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
        member _.Where(s: IMongoCollection<'T>, [<ProjectionParameter; ReflectedDefinition>] f) =
            {
                collection = s
                filters = Builders<_>.Filter.Where(f)
            }

        [<CustomOperation("where", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
        member _.Where(ctx: BaseMongoContext<'T>, [<ProjectionParameter; ReflectedDefinition>] f) =
            let clause = FilterDefinition.op_Implicit(predicate=f)
            { ctx with filters = ctx.filters &&& clause }

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


    type MongoUpdateBuilder() =
        inherit BaseMongoBuilder()

        [<CustomOperation("update", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
        member this.Update(
            ctx: BaseMongoContext<'T>, 
            [<ProjectionParameter; ReflectedDefinition>] f: Expression<Func<'T, SetUpdate<'a>>>) =
            let struct (value, update) = MongoExpressionParser.ToExpression(f)

            let builder = UpdateDefinitionBuilder<'T>().Set(update, value)
            { query = ctx; update = builder }

        [<CustomOperation("update", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
        member this.Increment(
            ctx: BaseMongoContext<'T>, 
            [<ProjectionParameter; ReflectedDefinition>] f: Expression<Func<'T, IncrUpdate<'a>>>) =
            let struct (value, update) = MongoExpressionParser.ToExpression(f)

            let builder = UpdateDefinitionBuilder<'T>().Inc(update, value)
            { query = ctx; update = builder }

        [<CustomOperation("update", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
        member this.Update(
            ctx: MongoUpdateContext<'T>, 
            [<ProjectionParameter; ReflectedDefinition>] f: Expression<Func<'T, SetUpdate<'a>>>) =
            let struct (value, update) = MongoExpressionParser.ToExpression(f)

            { ctx with update = ctx.update.Set(update, value) }


open Builder
module Explore =
    type Demo = { Id: string; Name: string; Version: int }

    let m = MongoQueryBuilder()
    let explore (eff: IMongoCollection<Demo>) =
        let t =
            m {
                for v in eff do
                    where (v.Id = "")
                    where (v.Name = "")
                    limit (10)
                    sortBy (v.Name)
                    //select ({| test = v.Id |})
                    //with_options ignore
            }
        ()

    let m2 = MongoUpdateBuilder()
    let explore2 (eff: IMongoCollection<Demo>) =
        let t =
            m2 {
                for v in eff do
                    where (v.Id = "")
                    where (v.Name = "")
                    update (v.Version += 2)

            }
        ()
