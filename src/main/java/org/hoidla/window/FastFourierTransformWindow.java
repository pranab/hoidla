/*
 * hoidla: various algorithms for Big Data solutions
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


package org.hoidla.window;

import org.chombo.math.Complex;
import org.hoidla.analyze.FastFourierTransform;


/**
 * @author pranab
 *
 */
public class FastFourierTransformWindow extends SizeBoundWindow<Double> {
	private Complex[] fftOutput;
	private double amp[];
	private double[] phase;
	
	/**
	 * @param maxSize
	 */
	public FastFourierTransformWindow(int maxSize) {
		super(maxSize, maxSize, maxSize);
	}
	
	/* (non-Javadoc)
	 * @see org.hoidla.window.DataWindow#processFullWindow()
	 */
	public  void processFullWindow() {
		Complex[] fftInput = new Complex[maxSize];
		for (int i = 0; i < maxSize; ++i) {
			fftInput[i] = new Complex(dataWindow.get(i), 0);
		}
		fftOutput = FastFourierTransform.fft(fftInput);
		amp = FastFourierTransform.findAmp(fftOutput);
		phase = FastFourierTransform.findPhase(fftOutput);
	}

	/**
	 * @return
	 */
	public Complex[] getFft() {
		return fftOutput;
	}

	/**
	 * @return
	 */
	public double[] getAmp() {
		return amp;
	}

	/**
	 * @return
	 */
	public double[] getPhase() {
		return phase;
	}


}
