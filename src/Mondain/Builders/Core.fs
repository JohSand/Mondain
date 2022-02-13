namespace Mondain.Builders

open System
open System.Collections.Generic
open System.Runtime.CompilerServices

open Mondain

open MongoDB.Driver
open MongoDB.Bson.Serialization

[<AutoOpen>]
module Extensions =
    [<Extension>]
    type SeqExtensions =
        [<Extension>]
        static member arrayFilter(_s: IEnumerable<'T>, _i: Func<'T, bool>): 'T =
            failwith "This is only intended to be used as part of an array filter in the mongo DSL."

type Update<'a> = 
    private 
    | SetUpdate of 'a
    | IncrUpdate of 'a
    | MulUpdate of 'a
    | BitwiseOrUpdate of 'a
    | BitwiseAndUpdate of 'a
    | BitwiseXorUpdate of 'a
    | PushUpdate of 'a
    | PushEachUpdate of IEnumerable<'a>

    //add all update operators?
  //member AddToSet<'TItem> : field: Expression<System.Func<'TDocument,IEnumerable<'TItem>>> * value: 'TItem -> UpdateDefinition<'TDocument>
  //member AddToSetEach<'TItem> : field: Expression<System.Func<'TDocument,IEnumerable<'TItem>>> * values: IEnumerable<'TItem> -> UpdateDefinition<'TDocument>

  //member CurrentDate: field: Expression<System.Func<'TDocument,obj>> * ?``type`` : System.Nullable<UpdateDefinitionCurrentDateType> -> UpdateDefinition<'TDocument>
  //member Max<'TField> : field: Expression<System.Func<'TDocument,'TField>> * value: 'TField -> UpdateDefinition<'TDocument>
  //member Min<'TField> : field: Expression<System.Func<'TDocument,'TField>> * value: 'TField -> UpdateDefinition<'TDocument>
  //member PopFirst: field: Expression<System.Func<'TDocument,obj>> -> UpdateDefinition<'TDocument>
  //member PopLast: field: Expression<System.Func<'TDocument,obj>> -> UpdateDefinition<'TDocument>
  //member Pull<'TItem> : field: Expression<System.Func<'TDocument,IEnumerable<'TItem>>> * value: 'TItem -> UpdateDefinition<'TDocument>
  //member PullAll<'TItem> : field: Expression<System.Func<'TDocument,IEnumerable<'TItem>>> * values: IEnumerable<'TItem> -> UpdateDefinition<'TDocument>

  //member PullFilter<'TItem> : field: Expression<System.Func<'TDocument,IEnumerable<'TItem>>> * filter: Expression<System.Func<'TItem,bool>> -> UpdateDefinition<'TDocument>

  //member Rename: field: Expression<System.Func<'TDocument,obj>> * newName: string -> UpdateDefinition<'TDocument>

  //member Set<'TField> : field:         Expression<System.Func<'TDocument,'TField>> * value: 'TField -> UpdateDefinition<'TDocument>

  //member SetOnInsert<'TField> : field: Expression<System.Func<'TDocument,'TField>> * value: 'TField -> UpdateDefinition<'TDocument>

  //member Unset: field: Expression<System.Func<'TDocument,obj>> -> UpdateDefinition<'TDocument>

    member internal this.ToUpdateDefinition(update: FieldDefinition<_, _>) =
        let b = Builders<'T>.Update

        match this with
        | SetUpdate value -> b.Set(update, value)
        | IncrUpdate value -> b.Inc(update, value)
        | MulUpdate value -> b.Mul(update, value)
        | BitwiseOrUpdate value -> b.BitwiseOr(update, value)
        | BitwiseAndUpdate value -> b.BitwiseAnd(update, value)
        | BitwiseXorUpdate value -> b.BitwiseXor(update, value)
        | PushUpdate values -> b.Push(update, values)
        | PushEachUpdate values -> b.PushEach(update, values)

[<AutoOpen>]
module Operators =
    //slightly sketch?
    let (:=) (_: 'a) (a: 'a) = SetUpdate a
    let (+=) (_: 'a) (a: 'a) = IncrUpdate a
    let ( *= ) (_: 'a) (a: 'a) = MulUpdate a
    let ( |||| ) (_: 'a) (a: 'a) = BitwiseOrUpdate a
    let ( &&&& ) (_: 'a) (a: 'a) = BitwiseAndUpdate a
    let ( ^^^^ ) (_: 'a) (a: 'a) = BitwiseXorUpdate a

    let ( ++ ) (_: IEnumerable<'a>) (a: 'a) = PushUpdate a
    let ( @@ ) (_: IEnumerable<'a>) (a: IEnumerable<'a>) = PushEachUpdate a


module Core =
    open System.Linq.Expressions

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
            match mce.Arguments[0], mce.Arguments[1] with
            | :? MemberExpression as mex, (:? LambdaExpression as lex) ->        
                let randomName = $"filter_{counter}"
                let counter = counter + 1
                let filters =
                    NamedArrayFilterDefinition(randomName, lex) :> ArrayFilterDefinition :: filters
    
                let fragments = resolve mex :: $"$[{randomName}]" :: pathFragments
    
                parseMember mex.Expression fragments filters counter
            | _ -> (pathFragments, filters)
    
        | _ -> (pathFragments, filters)
    
    let checkArrayFilters exp count =
        let a, b = parseMember exp [] [] count
        String.Join(".", a), (List.toArray b)

    let createDefinition(e: Expression<Func<'T, Update<'a>>>) count : struct (FieldDefinition<'T, 'a> * Update<'a> * _) =
        match e.Body with
        | :? MethodCallExpression as mce ->
            let lhs = mce.Arguments[0]
            let rhs = mce.Arguments[1] :?> ConstantExpression
            let value = rhs.Value :?> 'a
            let update = 
                match mce.Method.Name with
                | "op_ColonEquals" -> SetUpdate value
                | "op_AdditionAssignment" -> IncrUpdate value
                | _ -> failwith "stop creating wrapper types in unintended ways."

            let (setExpression, filters) = checkArrayFilters lhs count

            let fieldDef = 
                if filters.Length > 0 then
                    FieldDefinition<_, _>.op_Implicit setExpression
                else
                    Expression.Lambda<Func<_, _>>(lhs, e.Parameters[0]) 
                    |> ExpressionFieldDefinition<_, _> 
                    :> FieldDefinition<_,_>

            struct (fieldDef, update, filters)
        | _ -> failwith "stop creating wrapper types in unintended ways."

    let createUpdate f count =
        let struct (update, value, filters) = createDefinition f count
        let updateDef = value.ToUpdateDefinition(update)
        struct (updateDef, filters)