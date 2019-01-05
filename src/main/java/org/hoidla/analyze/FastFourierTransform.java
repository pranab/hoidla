
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
 * Credit: Robert Sedgewick and Kevin Wayne of Princeton University
 */

package org.hoidla.analyze;

import org.chombo.math.Complex;

/**
 * fast fourier transforms
 * @author pranab
 *
 */
public class FastFourierTransform {
	
	/**
	 * compute the FFT of x[], assuming its length is a power of 2
	 * @param x
	 * @return
	 */
	public static Complex[] fft(Complex[] x) {
		int n = x.length;

		// base case
		if (n == 1) 
			return new Complex[] { x[0] };

        // radix 2 Cooley-Tukey FFT
		if (n % 2 != 0) {
			throw new IllegalArgumentException("n is not a power of 2");
		}

		// fft of even terms
		Complex[] even = new Complex[n/2];
		for (int k = 0; k < n/2; k++) {
			even[k] = x[2*k];
		}
		Complex[] q = fft(even);

        // fft of odd terms
		Complex[] odd  = even;  // reuse the array
		for (int k = 0; k < n/2; k++) {
			odd[k] = x[2*k + 1];
		}
		Complex[] r = fft(odd);

        // combine
		Complex[] y = new Complex[n];
		for (int k = 0; k < n/2; k++) {
			double kth = -2 * k * Math.PI / n;
			Complex wk = new Complex(Math.cos(kth), Math.sin(kth));
			y[k] = q[k].plus(wk.times(r[k]));
			y[k + n/2] = q[k].minus(wk.times(r[k]));
		}
		return y;
    }


	/**
	 * compute the inverse FFT of x[], assuming its length is a power of 2
	 * @param x
	 * @return
	 */
	public static Complex[] ifft(Complex[] x) {
		int n = x.length;
		Complex[] y = new Complex[n];

		// take conjugate
		for (int i = 0; i < n; i++) {
			y[i] = x[i].conjugate();
		}

		// compute forward FFT
		y = fft(y);

		// take conjugate again
		for (int i = 0; i < n; i++) {
			y[i] = y[i].conjugate();
		}

		// divide by n
		for (int i = 0; i < n; i++) {
			y[i] = y[i].scale(1.0 / n);
		}

		return y;
    }

	/**
	 * compute the circular convolution of x and y
	 * @param x
	 * @param y
	 * @return
	 */
	public static Complex[] cconvolve(Complex[] x, Complex[] y) {

		// should probably pad x and y with 0s so that they have same length
		// and are powers of 2
		if (x.length != y.length) {
			throw new IllegalArgumentException("Dimensions don't agree");
		}

		int n = x.length;

		// compute FFT of each sequence
		Complex[] a = fft(x);
		Complex[] b = fft(y);

		// point-wise multiply
		Complex[] c = new Complex[n];
		for (int i = 0; i < n; i++) {
			c[i] = a[i].times(b[i]);
		}

		// compute inverse FFT
		return ifft(c);
	}


 	/**
	 * compute the linear convolution of x and y
	 * @param x
	 * @param y
	 * @return
	 */
	public static Complex[] convolve(Complex[] x, Complex[] y) {
		Complex ZERO = new Complex(0, 0);

		Complex[] a = new Complex[2*x.length];
		for (int i = 0;        i <   x.length; i++) a[i] = x[i];
		for (int i = x.length; i < 2*x.length; i++) a[i] = ZERO;

		Complex[] b = new Complex[2*y.length];
		for (int i = 0;        i <   y.length; i++) b[i] = y[i];
		for (int i = y.length; i < 2*y.length; i++) b[i] = ZERO;

		return cconvolve(a, b);
	}

	/**
	 * display an array of Complex numbers to standard output
	 * @param x
	 * @param title
	 */
	public static void show(Complex[] x, String title) {
		System.out.println(title);
		System.out.println("-------------------");
		for (int i = 0; i < x.length; i++) {
			System.out.println(x[i]);
		}
		System.out.println();
	}	
	
	
	
	public static void main(String[] args) { 
        int n = 8;
        Complex[] x = new Complex[n];

        // original data
        for (int i = 0; i < n; i++) {
            x[i] = new Complex(i, 0);
            x[i] = new Complex(-2*Math.random() + 1, 0);
        }
        show(x, "x");

        // FFT of original data
        Complex[] y = fft(x);
        show(y, "y = fft(x)");

        // take inverse FFT
        Complex[] z = ifft(y);
        show(z, "z = ifft(y)");

        // circular convolution of x with itself
        Complex[] c = cconvolve(x, x);
        show(c, "c = cconvolve(x, x)");

        // linear convolution of x with itself
        Complex[] d = convolve(x, x);
        show(d, "d = convolve(x, x)");
    }	

}
