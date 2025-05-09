#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# hawkins_young.py
# last updated: 2024-05-08

import numpy as np
from scipy.optimize import least_squares
from scipy.special import erfinv
from scipy.special import erf
from scipy.stats import uniform
from scipy.stats import lognorm
from scipy.stats import t


def geoSD(x):
    """Compute geometric standard deviation and the associated decimal
    coefficients to provide high and low range values about the mean.

    For a series of values X, which are log-normal, such that Z = log(X)
    is normally distributed about (mu, sigma).

    The geometric mean (mu_g) of X is e^(mu) and geometric standard
    deviation (std_g) is e^(sigma).

    Unlike standard deviation, the geometric standard deviation is a
    non-negative, unitless multiplicative factor that acts as a measure of
    the spread of logarithmic values around the mean. Therefore, the spread
    is given as: mu_g/sigma_g to mu_g*sigma_g. This contrasts standard
    deviation, which is mu +/- sdev.

    Analogous to Pearson's coefficient of variation, the geometric
    coefficient of variation (GCV) provides the multiplicative factor to
    be used against X is:

    GCV (%) = (GSD - 1)*100

    with the range (100*100/(100+GSC), 100+GCV) in percentages.

    Parameters
    ----------
    x : list, numpy.array, or iterable
        A list of numeric values representing X.

    Returns
    -------
    dict
        Geometric mean (mu_g), geometric standard deviation (sigma_g),
        geometric coefficient of variation (gcv), the upper/lower
        coefficients (to multiply against mu_g), and upper/lower limits
        (after multiplying against mu_g).

    Notes
    -----
    Provides several variants of the geometric standard deviation
    calculation, all providing the same value.

    Reference:
        Kirdwood (1970), "Geometric means and measures of dispersion,"
        Biometrics, 35(4), 908--909. https://www.jstor.org/stable/2530139

    Examples
    --------
    >>> rv = scipy.stats.lognorm.rvs(s=0.95, scale=0.5, loc=693, size=5000)
    >>> params = geoSD(rv)
    >>> print("%0.3f,%0.3f,%0.3f" % (
    ...     params['lower_limit'], params['mu_g'], params['upper_limit']))
    692.843,693.786,694.730
    """
    # Convert to floats in a numpy array
    x = np.array(x, dtype=np.float64)

    # Remove negatives and non-finite values
    x[x <= 0] = np.nan
    x_finite = np.isfinite(x)
    x = x[x_finite]
    n = len(x)

    # Transform data
    xlog = np.log(x)

    # Compute geometric mean and standard dev.
    mu_g = np.exp(xlog.mean())

    # NOTE: sigma_g's lower bound is 1 because exp to any positive
    # number is >=1 and stdev (i.e., square root of variance) is
    # always positive.

    # Scipy's method; note that ddof is zero,
    # where ddof determines the degress of freedom = N-ddof
    sigma_g3 = np.exp(np.std(xlog, ddof=0))
    # BIO-RSG/oceancolouR method
    # https://rdrr.io/github/BIO-RSG/oceancolouR/src/R/math_funcs.R
    sigma_g2 = np.exp(np.sqrt(np.sum(np.log(x/mu_g)**2)/n))
    # codellama's translation (yep, that's the same):
    sigma_g1 = np.exp(np.sqrt(np.var(xlog, ddof=0, axis=0)))
    # Short-hand of sigma_g2, note the ddof needs to be 1
    sigma_g = np.exp(np.sqrt((n-1)/n) * np.std(xlog, ddof=1))

    # Geometric coefficient of variation, GCV, expressed as a
    # percentage. This provides you with the limits of the range
    # of values about the mu_g.
    gcv = 100*(sigma_g - 1)
    r1 = 100*100/(100 + gcv)
    r2 = 100+gcv

    return ({
        'mu_g': mu_g,
        'sigma_g': (sigma_g3, sigma_g2, sigma_g1, sigma_g),
        'gcv_%': gcv,
        'gcv': gcv/100.0,
        'lower_coef_%': r1,
        'lower_coef': r1/100.0,
        'upper_coef_%': r2,
        'upper_coef': r2/100.0,
        'lower_limit': mu_g*r1/100.0,
        'upper_limit': mu_g*r2/100.0,
    })


def hawkins_young_sigma(x, **kwargs):
    # From Young et al. (2019) <https://doi.org/10.1021/acs.est.8b05572>,
    # to ensure non-negative releases in Monte Carlo simulations, the error
    # is set to a log-normal distribution with the expected value assigned
    # to the emission factor and the 95th percentile of the cumulative
    # distribution function (CDF) set to the 90% confidence interval upper
    # limit (CIU).
    # Based on the CDF for lognormal distribution, D(x), set to 0.95 for
    # x = EF*(1+PI), the 90% CIU based on a given emission factor, EF, and
    # prediction/confidence interval expressed as a fraction (or percentage);
    # hence the 1+CIU. If CIU is undefined, a default value of 50% is used.
    if 'alpha' in kwargs.keys():
        alpha = kwargs['alpha']
    else:
        alpha = 0.9

    if 'ciu' in kwargs.keys():
        ciu = kwargs['ciu']
    else:
        ciu = 0.5

    a = 0.5
    z = erfinv(alpha)
    b = -2**0.5*z
    c = np.log(1 + ciu)
    r = a*x**2 + b*x + c
    return r


def hawkins_young(data, ef, alpha):
    # From Young et al. (2019) <https://doi.org/10.1021/acs.est.8b05572>,
    # the prediction interval is expressed as the percentage of the expected
    # release factor; Eq 3. expresses it as:
    # P = s * sqrt(1 + 1/n)*z/y_hat
    # where:
    #   s is the standard error of the expected value, SEM
    #   n is the sample size
    #   z is the critical value for 90% confidence
    #   y_hat is the expected value
    # Note that there is no assumed log-normal distribution here.
    n = len(data)
    z = t.ppf(q=alpha, df=n-1)
    se = np.std(data)/np.sqrt(n)
    y_hat = data.mean()
    ciu = se*np.sqrt(1 + 1/n)*z/y_hat

    # Use least-squares fitting for the quadratic.
    # NOTE: remember, we are fitting sigma, the standard deviation of the
    #       underlying normal distribution. A 'safe' assumption is to
    #       expect sigma to be between 1 and 5. So run a few fits and
    #       get the one that isn't negative (most positive).
    #       Alternatively, we could take std(ddof=1) of the log of the data
    #       to get an estimate of the standard deviation and search across
    #       4x's of it. See snippet code for method:
    #       `s_std = np.round(4*np.log(data).std(ddof=1), 0)`
    all_ans = []
    for i in uniform.rvs(0, 6, size=10):
        ans = least_squares(
            hawkins_young_sigma, i, kwargs={'alpha': alpha, 'ciu': ciu})
        all_ans.append(ans['x'][0])

    sigma = np.max(all_ans)
    mu = np.log(ef) - 0.5*sigma**2

    mu_g = np.exp(mu)
    sigma_g = np.exp(sigma)

    return {
        'mu': mu,
        'sigma': sigma,
        'mu_g': mu_g,
        'sigma_g': sigma_g,
        'ci_%': ciu*100,
    }


def estMethod1(x):
    # Abramowitz & Stegun. Handbook of Mathematical Equations.
    # Eq. 7.1.27; setting the error function to 0.9.
    # Basically returns scipy.special.erfinv(0.9) = 1.163087
    a1 = 0.278393
    a2 = 0.230389
    a3 = 0.000972
    a4 = 0.078108
    r = 1.0 - 1.0/(1 + a1*x + a2*x**2 + a3*x**3 + a4*x**4)**4
    return 0.9 - r


def estMethod2(x):
    # Abramowitz & Stegun. Handbook of Mathematical Equations.
    # Eq. 7.1.27; setting the error function to 0.9.
    # Basically returns scipy.special.erfinv(0.9) = 1.163087
    a1 = 0.0705230784
    a2 = 0.0422820123
    a3 = 0.0092705272
    a4 = 0.0001520143
    a5 = 0.0002765672
    a6 = 0.0000430638
    r = 1.0
    r -= 1.0/(1 + a1*x + a2*x**2 + a3*x**3 + a4*x**4 + a5*x**5 + a6*x**6)**16
    return 0.9 - r


if __name__ == '__main__':
    # Just some data AI made for me:
    data = np.array([0.43, 1.23, 2.15, 4.67, 6.89, 8.12, 11.35, 14.78, 18.21, 22.65, 27.09, 32.53, 38.97, 45.42, 52.87])

    # Prove to yourself that the error function estimations are just
    # giving you the inverse error function value of 0.9.
    em1 = least_squares(estMethod1, 1.5)
    em2 = least_squares(estMethod2, 1.5)
    e90 = erfinv(0.9)
    print("Estimate 1: %0.5f" % em1['x'][0])
    print("Estimate 2: %0.5f" % em2['x'][0])
    print("Inv. erf:   %0.5f" % e90)

    # Run the Hawkins-Young method
    # NOTE: we set the emission factor, EF, to the regional sum
    # and I kept alpha (confidence level) a parameter.
    ef = data.sum()
    alpha = 0.9
    results = hawkins_young(data, ef, alpha)

    # Extract the results and observe:
    mu = results['mu']
    sigma = results['sigma']
    ci = results['ci_%']
    hss = 0.5*sigma**2   # half sigma squared (hss)
    srt = np.sqrt(2)     # square-root of two (srt)
    error = 0.5*(1 + erf((np.log(1 + ci/100.) + hss)/(srt*sigma)))
    print("E(x) = %0.3f (%0.3f)" % (np.exp(mu + hss), ef))
    print("D(x) = %0.3f (0.95)" % error)

    # Generate a distribution with the same properties:
    # exp(mu) ~ scale; sigma ~ shape
    my_dist = lognorm.rvs(sigma, scale=np.exp(mu), size=1000)
    my_fit = lognorm.fit(my_dist, floc=0)

    # A second test dataset
    data = np.random.lognormal(0.25, 1.25, size=19)
    data *= 250
