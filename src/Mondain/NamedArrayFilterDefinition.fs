namespace Mondain

open MongoDB.Bson
open MongoDB.Driver
open System.Linq
open System.Linq.Expressions
open System

open Mondain.ExpressionParsing
        

type NamedArrayFilterDefinition(filterName: string, filterExpr: Expression) =
    inherit ArrayFilterDefinition()
    let ``type`` = filterExpr.Type.GenericTypeArguments.[0]
    override _.ItemType = ``type``
    //since we wont know the type at compile time
    member _.BaseFilterDefinitionType = typedefof<ExpressionFilterDefinition<_>>.MakeGenericType(``type``)
    
    override this.Render(_s, r, p) =
        //this is ExpressionFilterDefinition<``type``>, but we don't know type at compile time.
        let s = r.GetSerializer(``type``)

        let filterDef =
            constructorCache
                .GetOrAdd(
                    this.BaseFilterDefinitionType,
                    fun (t: Type) -> t.GetConstructor(types = [| filterExpr.GetType() |]) |> compileCtor
                )
                .Invoke([| filterExpr |])

        //use the same Render as ExpressionFilterDefinition.
        let filter =
            renderMethodCache
                .GetOrAdd(
                    this.BaseFilterDefinitionType,
                    fun t -> t.GetMethod("Render", [| s.GetType(); r.GetType(); p.GetType() |]) |> compileMethod
                )
                .Invoke(filterDef, [| s; r; p |]) :?> BsonDocument

        //add the name of the filter
        filter.Names
            .Zip(filter.Values, (fun eName value -> (filterName + "." + eName, value)))   
            .ToDictionary(fst, snd)
            |> BsonDocument
