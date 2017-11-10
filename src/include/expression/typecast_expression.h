//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// typecast_expression.h
//
// Identification: src/include/expression/typecast_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "expression/abstract_expression.h"
#include "common/sql_node_visitor.h"
#include "type/value_factory.h"

namespace peloton {
  namespace expression {

//===----------------------------------------------------------------------===//
// OperatorExpression
//===----------------------------------------------------------------------===//
    class TypecastExpression : public AbstractExpression {

    public:
      TypecastExpression(AbstractExpression *inner, type::TypeId type_id) : AbstractExpression(ExpressionType::CAST),
                                                                            type_id_(type_id) {
        if (inner != nullptr) children_.push_back(std::unique_ptr<AbstractExpression>(inner));
      }


      type::Value Evaluate(const AbstractTuple *, const AbstractTuple *,
                           executor::ExecutorContext *) const override {
        // TODO(tianyu) implement
        throw std::runtime_error("Not implemented");
      }

      AbstractExpression *Copy() const override {
        return new TypecastExpression(*this);
      }

      void Accept(SqlNodeVisitor *visitor) override { visitor->Visit(this); }

    private:
      type::TypeId type_id_;

    };
  }
}