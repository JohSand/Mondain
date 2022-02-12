namespace Mondain

open MongoDB.Bson
open MongoDB.Driver
open System.Linq
open System.Linq.Expressions

type NamedArrayFilterDefinition(filterName: string, filterExpr: Expression) =
    inherit ArrayFilterDefinition()
    let ``type`` = filterExpr.Type.GenericTypeArguments.[0]
    override _.ItemType = ``type``
    //since we wont know the type at compile time
    member _.BaseFilterDefinitionType = typedefof<ExpressionFilterDefinition<_>>.MakeGenericType(``type``)
    //we could gain perf here by caching/compiling the methods instead of always using reflection.
    override this.Render(_s, r, p) =
        //this is ExpressionFilterDefinition<``type``>, but we don't know type at compile time.
        let fd = this.BaseFilterDefinitionType.GetConstructor([| filterExpr.GetType() |]).Invoke([| filterExpr |])
        //use the same Render as ExpressionFilterDefinition.
        let filter = this.BaseFilterDefinitionType
                         .GetMethod("Render")
                         .Invoke(fd, [| r.GetSerializer(``type``); r; p |]) 
                         :?> BsonDocument
        //add the name of the filter
        filter.Names
            .Zip(filter.Values, (fun eName value -> (filterName + "." + eName, value)))   
            .ToDictionary(fst, snd)
            |> BsonDocument
