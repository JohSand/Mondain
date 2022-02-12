module Mondain.ExpressionParsing 

open System
open System.Linq.Expressions
open MongoDB.Bson.Serialization
open MongoDB.Driver
open Mondain

let serializerRegistry = BsonSerializer.SerializerRegistry

let private resolve (mex: MemberExpression) =
    let memberName = mex.Member.Name
    match serializerRegistry.GetSerializer(mex.Member.ReflectedType) with
    | :? IBsonDocumentSerializer as ser ->
        match ser.TryGetMemberSerializationInfo(memberName) with
        | true, resx -> resx.ElementName
        | false, _ -> memberName
    | _ -> memberName

let rec private parseMember (body: Expression) pathFragments filters counter =
    match body with
    | :? LambdaExpression as l ->
        parseMember l.Body pathFragments filters counter
    | :? MemberExpression as mex ->
        parseMember mex.Expression (resolve mex :: pathFragments) filters counter
    | :? MethodCallExpression as mce when (mce.Method.Name = "arrayFilter") ->
        match mce.Arguments |> List.ofSeq with
        | [ :? MemberExpression as mex; :? LambdaExpression as lex ] ->        
            //todo
            let randomName = $"filter_{counter}"
            let counter = counter + 1
            let filters =
                NamedArrayFilterDefinition(randomName, lex) :> ArrayFilterDefinition :: filters

            let fragments = resolve mex :: $"$[{randomName}]" :: pathFragments

            parseMember mex.Expression fragments filters counter
        | _ -> (pathFragments, filters)

    | _ -> (pathFragments, filters)

let checkArrayFilters(exp) =
    let a, b = parseMember exp [] [] 1
    String.Join(".", a), (List.toArray b)

