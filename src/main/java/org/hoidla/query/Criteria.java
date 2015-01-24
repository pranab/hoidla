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
			String operand = pred.getOperand();
			if (pred.isOperandScalar()) {
				//operand is a scalar stats summary of data
				double opValue = getOperandValue(operand);
				if (i == 0) {
					//first operand
					result = pred.evaluate(opValue);
				} else if (operators.get(i - 1).equals(OPERATOR_AND)) {
					//continue with current conjunctive
					if (result) {
						result = result && pred.evaluate(opValue);
					}
				} else {
					//start new conjunctive
					conjuctResults.add(result);
					result = pred.evaluate(opValue);
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
		
		return finalResult;
	}
	
	/**
	 * @param operand
	 * @return
	 */
	private double getOperandValue(String operand) {
		//try cache first
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
			}
			operandValues.put(operand, opValue);
		}
		
		return opValue;
	}
}
