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

/**
 * Evaluates simple predicate
 * @author pranab
 *
 */
public class Predicate {
	private String operand;
	private String operator;
	private double value;
	private int percentTrue;
	
	public static final String OPERATOR_GT = "gt";
	public static final String OPERATOR_LT = "lt";
	public static final String OPERATOR_GE = "ge";
	public static final String OPERATOR_LE = "le";
	
	public static final String OPERAND_NONE = "none";
	public static final String OPERAND_MEAN = "mean";
	public static final String OPERAND_STD_DEV = "stdDev";
	public static final String OPERAND_MEDIAN = "median";
	public static final String OPERAND_ENTROPY = "entropy";
	
	
	public Predicate() {
		
	}
	
	/**
	 * @param operand
	 * @param operator
	 * @param value
	 */
	public Predicate(String operand, String operator, double value) {
		this.operand = operand;
		this.operator = operator;
		this.value = value;
		
	}
	
	/**
	 * @param operator
	 * @param value
	 */
	public Predicate(String operator, double value) {
		this.operator = operator;
		this.value = value;
	}

	public boolean evaluate(double[] data) {
		boolean result = false;
		
		return result;
	}
	
	/**
	 * @param operandValue
	 * @return
	 */
	public boolean evaluate(double operandValue) {
		boolean result = false;
		if (operator.equals(OPERATOR_GT)) {
			result = operandValue > value;
		} else if (operator.equals(OPERATOR_LT)) {
			result = operandValue < value;
		} else if (operator.equals(OPERATOR_GE)) {
			result = operandValue >= value;
		} else if (operator.equals(OPERATOR_LE)) {
			result = operandValue <= value;
		} else {
			throw new IllegalArgumentException("illegal operator");
		}
		return result;
	}

	public String getOperand() {
		return operand;
	}

	public void setOperand(String operand) {
		this.operand = operand;
	}

	public String getOperator() {
		return operator;
	}

	public void setOperator(String operator) {
		this.operator = operator;
	}

	public double getValue() {
		return value;
	}

	public void setValue(double value) {
		this.value = value;
	}
	
	public int getPercentTrue() {
		return percentTrue;
	}

	public void setPercentTrue(int percentTrue) {
		this.percentTrue = percentTrue;
	}

	public boolean isOperandScalar() {
		return !operand.equals(OPERAND_NONE);
	}
}
