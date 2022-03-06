namespace Mondain.Builders

open System
open System.Collections.Generic
open System.Runtime.CompilerServices
open System.Reflection

open Mondain
open Mondain.ExpressionParsing

open MongoDB.Driver
open System.Linq.Expressions

type Update<'a> = 
    private 
    | SetUpdate of 'a
    | SetOnInsertUpdate of 'a
    | IncrUpdate of 'a
    | MulUpdate of 'a
    | BitwiseOrUpdate of 'a
    | BitwiseAndUpdate of 'a
    | BitwiseXorUpdate of 'a
    | CurrentDateUpdate

    member internal this.ToUpdateDefinition(selector: FieldDefinition<_, _>) =
        let b = Builders<'T>.Update

        match this with
        | SetUpdate value -> b.Set(selector, value)
        | SetOnInsertUpdate value -> b.SetOnInsert(selector, value)
        | IncrUpdate value -> b.Inc(selector, value)
        | MulUpdate value -> b.Mul(selector, value)
        | BitwiseOrUpdate value -> b.BitwiseOr(selector, value)
        | BitwiseAndUpdate value -> b.BitwiseAnd(selector, value)
        | BitwiseXorUpdate value -> b.BitwiseXor(selector, value)
        | CurrentDateUpdate ->  
            //UpdateDefinitionCurrentDateType.Timestamp
            b.CurrentDate(selector)

//selector is IEnumerable
type SeqUpdate<'a> = 
    private 
    | PullUpdate of 'a
    | PullAllUpdate of seq<'a>
    | PullFilterUpdate of FilterDefinition<'a>
    | PushUpdate of 'a
    | PushEachUpdate of seq<'a>
    member internal this.ToUpdateDefinition(selector: FieldDefinition<'T, seq<'a>>) =
        let update = FieldDefinition<_,_>.op_Implicit selector
        let b = Builders<'T>.Update
        match this with
        | PullUpdate item -> b.Pull(update, item)
        | PullAllUpdate items -> b.PullAll(update, items)
        | PullFilterUpdate filter -> b.PullFilter(update, filter)
        | PushUpdate values -> b.Push(update, values)
        | PushEachUpdate values -> b.PushEach(update, values)

    //add all update operators?
  //member AddToSet<'TItem> : field: Expression<System.Func<'TDocument,IEnumerable<'TItem>>> * value: 'TItem -> UpdateDefinition<'TDocument>
  //member AddToSetEach<'TItem> : field: Expression<System.Func<'TDocument,IEnumerable<'TItem>>> * values: IEnumerable<'TItem> -> UpdateDefinition<'TDocument>

  //member Max<'TField> : field: Expression<System.Func<'TDocument,'TField>> * value: 'TField -> UpdateDefinition<'TDocument>
  //member Min<'TField> : field: Expression<System.Func<'TDocument,'TField>> * value: 'TField -> UpdateDefinition<'TDocument>
  //member PopFirst: field: Expression<System.Func<'TDocument,obj>> -> UpdateDefinition<'TDocument>
  //member PopLast: field: Expression<System.Func<'TDocument,obj>> -> UpdateDefinition<'TDocument>

  //member Rename: field: Expression<System.Func<'TDocument,obj>> * newName: string -> UpdateDefinition<'TDocument>

  //member Unset: field: Expression<System.Func<'TDocument,obj>> -> UpdateDefinition<'TDocument>



        

[<AutoOpen>]
module Extensions =
    [<Extension>]
    type SeqExtensions =
        [<Extension>]
        static member arrayFilter(_s: IEnumerable<'T>, _i: Func<'T, bool>): 'T =
            failwith "This is only intended to be used as part of an array filter in the mongo DSL."

        [<Extension>]
        static member removeItem<'TItem>(_: IEnumerable<'TItem>, item: 'TItem) : SeqUpdate<'TItem> =
            SeqUpdate.PullUpdate item
        [<Extension>]
        static member removeItems<'TItem>(_: IEnumerable<'TItem>, items: IEnumerable<'TItem>) : SeqUpdate<'TItem> =
            SeqUpdate.PullAllUpdate items
        [<Extension>]
        static member removeWhere<'T>(_: IEnumerable<'T>,  f: Func<'T, bool>) : SeqUpdate<'T> =
            failwith "This needs to be handled differently"

[<AutoOpen>]
module Operators =
    //slightly sketch?
    let ( := ) (_: 'a) (a: 'a) = SetUpdate a
    let ( @= ) (_: 'a) (a: 'a) = SetUpdate a
    let ( += ) (_: 'a) (a: 'a) = IncrUpdate a
    let ( *= ) (_: 'a) (a: 'a) = MulUpdate a
    let ( |||| ) (_: 'a) (a: 'a) = BitwiseOrUpdate a
    let ( &&&& ) (_: 'a) (a: 'a) = BitwiseAndUpdate a
    let ( ^^^^ ) (_: 'a) (a: 'a) = BitwiseXorUpdate a

    let ( ++ ) (_: IEnumerable<'a>) (a: 'a) : SeqUpdate<_> = PushUpdate a
    let ( @@ ) (_: IEnumerable<'a>) (a: IEnumerable<'a>) = PushEachUpdate a


module Core =    
    let checkArrayFilters exp count =
        let a, b = parseMember exp [] [] count (fun name lex -> NamedArrayFilterDefinition(name, lex) :> ArrayFilterDefinition)
        String.Join(".", a), (List.toArray b)

    let createFieldDefinition (filters: _[]) (setter: string) (body) (parameter: ParameterExpression) =
        if filters.Length > 0 then
            FieldDefinition<_, _>.op_Implicit setter
        else
            Expression.Lambda<Func<_, _>>(body, parameter) 
            |> ExpressionFieldDefinition<_, _> 
            :> FieldDefinition<_,_>

    let createDefinition<'T, 'a>(e: Expression<Func<'T, Update<'a>>>) count : struct (FieldDefinition<'T, 'a> * Update<'a> * _) =
        match e.Body with
        | :? MethodCallExpression as mce ->
            let selector = mce.Arguments[0]
            let updateExpr = mce.Arguments[1] 
            let update = 
                    let arg = construct updateExpr
                    let args = [| box Unchecked.defaultof<'a>; arg |]

                    updatorMethodCache
                        .GetOrAdd(mce.Method.Name, (fun _ mi -> compileMethod mi), mce.Method)    
                        .Invoke(Unchecked.defaultof<_>, args)
                        :?> Update<'a>

            let (setter, filters) = checkArrayFilters selector count

            let fieldDef = createFieldDefinition filters setter selector e.Parameters[0]

            struct (fieldDef, update, filters)
        | _ -> failwith "stop creating wrapper types in unintended ways."



    let createDefinitionMulti<'T, 'a>(e: Expression<Func<'T, SeqUpdate<'a>>>) count : struct (FieldDefinition<'T, seq<'a>> * SeqUpdate<'a> * _) =
        match e.Body with
        | :? MethodCallExpression as mce ->
            let selector = mce.Arguments[0]
            let updateExpr = mce.Arguments[1] 
            let update = 
                match mce.Method.Name with
                | "removeWhere" ->
                    let rhs = updateExpr :?> Expression<Func<'a, bool>>
                    SeqUpdate.PullFilterUpdate (FilterDefinition<_>.op_Implicit(rhs))
                | _ -> 
                    let arg = construct updateExpr
                    let arg =
                        match arg with 
                        //the array cast is not working the default way
                        | :? (obj[]) as value -> Array.ConvertAll(value, fun item -> item :?> 'a) |> box
                        | a -> a
                    let args = [| Unchecked.defaultof<obj>; arg |]
                    updatorMethodCache
                        .GetOrAdd(mce.Method.Name, (fun _ mi-> compileMethod mi), mce.Method)   
                        .Invoke(Unchecked.defaultof<_>, args)
                        :?> SeqUpdate<'a>

            let (setter, filters) = checkArrayFilters selector count

            let fieldDef = createFieldDefinition filters setter selector e.Parameters[0]
                                      
            struct (fieldDef, update, filters)
        | _ -> failwith "stop creating wrapper types in unintended ways."

    let createUpdate f count =
        let struct (update, value, filters) = createDefinition f count
        let updateDef = value.ToUpdateDefinition(update)
        struct (updateDef, filters)

    
    let createUpdateMulti f count =
        let struct (update, value, filters) = createDefinitionMulti f count
        let updateDef = value.ToUpdateDefinition(update)
        struct (updateDef, filters)
