module Mondain.Builders.Abstractions

open MongoDB.Driver

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

type MongoFindAndUpdateContext<'a> =
    {|
        query: BaseMongoContext<'a>
        update: UpdateDefinition<'a>
        options: FindOneAndUpdateOptions<'a>
    |}

type MongoFindAndUpdateContext<'a, 'b> =
    {|
        query: BaseMongoContext<'a>
        update: UpdateDefinition<'a>
        options: FindOneAndUpdateOptions<'a, 'b>
    |}

type MongoUpdateContext<'a> =
    {|
        query: BaseMongoContext<'a>
        update: UpdateDefinition<'a>
        options: UpdateOptions
    |}

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
