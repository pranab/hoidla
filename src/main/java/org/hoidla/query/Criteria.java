/*
 * hoidla: various streaming algorithms for Big Data solutions
 * Author: Pranab Ghosh
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hoidla.query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hoidla.window.SizeBoundWindow;
import org.hoidla.window.WindowUtils;

/**
 * Evaluates a disjunctive criteria consisting list of conjunctive predicates.
 * Essentially processes a list of predicates connected by and or or operators.
 * No support for comples expressions
 * @author pranab
 *
 */
public class Criteria implements Serializable {
	private List<Predicate> predicates = new ArrayList<Predicate>();
	private double[] data;
	private int[] intData;
	private Map<String, Double> operandValues = new HashMap<String, Double>();
	public static final String OPERATOR_AND = "and";
	public static final String OPERATOR_OR = "or";
	private List<String> operators = new ArrayList<String>();
	
	/**
	 * @return
	 */
	public List<Predicate> getPredicates() {
		return predicates;
	}

	/**
	 * @param predicates
	 */
	public void setPredicates(List<Predicate> predicates) {
		this.predicates = predicates;
	}
	
	/**
	 * @param predicate
	 * @return
	 */
	public Criteria withPredicate(Predicate predicate) {
		predicates.add(predicate);
		return this;
	}
	
	/**
	 * @return
	 */
	public Criteria withAnd() {
		operators.add(OPERATOR_AND);
		return this;
	}
	
	
	/**
	 * @return
	 */
	public Criteria withOr() {
		operators.add(OPERATOR_OR);
		return this;
	}

	/**
	 * creates simple criteria consisting of two predicated connected AND or OR
	 * @param expression
	 * @return
	 */
	public static Criteria createCriteriaFromExpression(String expression) {
		Criteria criteria = new Criteria();
		String[] predicateText = null;
		boolean withAnd = false;
		if (expression.contains(OPERATOR_AND)) {
			predicateText = expression.split(OPERATOR_AND);
			withAnd = true;
		} else if (expression.contains(OPERATOR_OR)) {
			predicateText = expression.split(OPERATOR_OR);
		} else {
			//simple one  predicate
		}
		
		if (null  != predicateText) {
			Predicate predOne = createPredicateFromExpression(predicateText[0]);
			criteria.withPredicate(predOne);
			criteria = withAnd ? criteria.withAnd() : criteria.withOr();
			Predicate predTwo = createPredicateFromExpression(predicateText[1]);
			criteria.withPredicate(predTwo);
		} else {
			Predicate pred = createPredicateFromExpression(expression);
			criteria.withPredicate(pred);
		}
		return criteria;
	}
	
	/**
	 * @param expression
	 * @return
	 */
	public static  Predicate createPredicateFromExpression(String expression) {
		String[] predParts = expression.split("\\s+");
		Predicate pred = new Predicate(predParts[0], predParts[1], Double.parseDouble( predParts[2]));
		return pred;
	}
	
	/**
	 * evaluates simple criteria
	 * @return
	 */
	public boolean evaluate(double[] operandValues) {
		boolean result = true;
		for (int i = 0; i < predicates.size();  ++i) {
			if (i == 0) {
				result = predicates.get(i).evaluate(operandValues[i]);
			} else {
				if (operators.get(i-1).equals(OPERATOR_AND)) {
					result = result && predicates.get(i).evaluate(operandValues[i]);
				} else {
					result = result ||  predicates.get(i).evaluate(operandValues[i]);
				}
			}
		}
		
		return result;
	}
	
	/**
	 * @param window
	 * @return
	 */
	public<T> boolean evaluate(SizeBoundWindow<T> window) {
		if (operators.size() != predicates.size() -1) {
			throw new IllegalArgumentException("invalid criteria expression");
		}
		
		List<Boolean> conjuctResults = new ArrayList<Boolean>();
		boolean result = true;
		data = WindowUtils.getDoubleArray(window);
		intData = WindowUtils.getIntArray(window);
		int i = 0;
		for (Predicate pred : predicates) {
			if (!pred.isRelational() && null == data) {
				data = WindowUtils.getDoubleArray(window);
			}
			
			String operand = pred.getOperand();
			if (pred.isOperandScalar()) {
				//operand is a scalar stats summary of data
				double opValue = pred.isRelational()? getOperandValue(pred) : 0;
				if (i == 0) {
					//first operand
					if (pred.isRelational()) {
						result = pred.evaluate(opValue);
					} else {
						result = pred.evaluate(data);
					}
				} else if (operators.get(i - 1).equals(OPERATOR_AND)) {
					//continue with current conjunctive
					if (result) {
						if (pred.isRelational()) {
							result = result && pred.evaluate(opValue);
						} else {
							result = result && pred.evaluate(data);
						}
					}
				} else {
					//start new conjunctive
					conjuctResults.add(result);
					if (pred.isRelational()) {
						result = pred.evaluate(opValue);
					} else {
						result = pred.evaluate(data);
					}
				}
			} else {
				//vector operation with raw data
				int trueCount = 0;
				for (double value : data) {
					if (pred.evaluate(value)) {
						++trueCount;
					}
				}
				int minTrueCount = (pred.getPercentTrue() * window.size()) / 100;
				if (operators.get(i).equals(OPERATOR_AND)) {
					//continue with current conjunctive
					if (result) {
						result = result && trueCount >= minTrueCount;
					}
				}else {
					//start new conjunctive
					conjuctResults.add(result);
					result = true;
				}
			}
			++i;
		}
		
		//last conjunct
		conjuctResults.add(result);
		
		//process conjuctives
		boolean finalResult = false;
		for (boolean conRes : conjuctResults ) {
			finalResult = finalResult || conRes;
			if (finalResult) {
				break;
			}
		}
		
		operandValues.clear();
		return finalResult;
	}
	
	/**
	 * @param operand
	 * @return
	 */
	private double getOperandValue(Predicate pred) {
		//try cache first
		String operand = pred.getOperand();
		Double opValue = operandValues.get(operand);
		if (null == opValue) {
			if (operand.equals(Predicate.OPERAND_MEAN)) {
				opValue = WindowUtils.getMean(data);
			} else if (operand.equals(Predicate.OPERAND_STD_DEV)) {
				opValue = WindowUtils.getStdDev(data);
			} else if (operand.equals(Predicate.OPERAND_MEDIAN)) {
				opValue = WindowUtils.getMean(data);
			} else if (operand.equals(Predicate.OPERAND_ENTROPY)) {
				opValue = WindowUtils.getEntropy(intData);
			} else {
				throw new IllegalArgumentException("invalid operand:" + operand);
			}
			operandValues.put(operand, opValue);
		}
		
		return opValue;
	}
	
	public int getNumPredicates() {
		return predicates.size();
	}
}
