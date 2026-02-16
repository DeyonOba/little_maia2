import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import norm
import pandas as pd
from pyfonts import set_default_font, load_google_font


def create_cdf_pdf_plot(path: str, rating: int|float):
    font = load_google_font("Poppins")
    bold_font = load_google_font("Poppins", weight=400)
    set_default_font(font)

    arr = pd.read_table(path, header=None)
    ratings = arr.iloc[:, 0]

    # Calculate the mean and standard deviation from the ratings data
    mu_ratings = ratings.mean()
    sigma_ratings = ratings.std()
    
    # Calculate the cumulative probability (X <= rating)
    p_rating = np.sum(ratings <= rating) / len(ratings)


    # Generate data for a normal distribution based on the calculated mean and std dev
    # The x-range is chosen to cover most of the distribution (approx. +/- 4 standard deviations)
    x_ratings = np.linspace(mu_ratings - 4 * sigma_ratings, mu_ratings + 4 * sigma_ratings, 1000)

    # Calculate the cdf and pdf
    cdf_ratings = norm.cdf(x_ratings, mu_ratings, sigma_ratings)
    pdf_ratings = norm.pdf(x_ratings, mu_ratings, sigma_ratings)


    # Create the figure with two subplots, sharing the x-axis
    fig, (ax_pdf, ax_cdf) = plt.subplots(2, 1, sharex=True, figsize=(10, 8))
    fig.suptitle(f'Normal Distribution PDF and CDF of Ratings (Mean: {mu_ratings:.2f}, Std Dev: {sigma_ratings:.2f})', fontsize=14, font=bold_font)

    # --- Plot PDF on the top subplot (ax_pdf) ---
    ax_pdf.plot(x_ratings, pdf_ratings, label='Normal Distribution (PDF)', color='blue')
    ax_pdf.set_ylabel('Probability Density (PDF)', color='blue')
    ax_pdf.tick_params(axis='y', labelcolor='blue')
    ax_pdf.grid(False)
    ax_pdf.set_title('Probability Density Function (PDF)')

    # Add vertical line at rating
    ax_pdf.axvline(rating, color='gray', linestyle='--', label=f'Rating = {rating}')

    # Shade the area below the specified rating
    ax_pdf.fill_between(x_ratings, 0, pdf_ratings, where=(x_ratings <= rating), color='lightblue', alpha=0.5, label=f'Rating < {rating}')

    # Add text for mean and standard deviation
    ax_pdf.text(0.05, 0.90, f'$\mu$ = {mu_ratings:.2f}', transform=ax_pdf.transAxes, verticalalignment='top', fontsize=12);
    ax_pdf.text(0.05, 0.80, f'$\sigma$ = {sigma_ratings:.2f}', transform=ax_pdf.transAxes, verticalalignment='top', fontsize=12);
    ax_pdf.legend()


    # --- Plot CDF on the bottom subplot (ax_cdf) ---
    ax_cdf.plot(x_ratings, cdf_ratings, label='Cumulative Distribution (CDF)', color='red')
    ax_cdf.set_xlabel('Rating') # X-label only on the bottom plot
    ax_cdf.set_ylabel('Cumulative Probability (CDF)', color='red')
    ax_cdf.tick_params(axis='y', labelcolor='red')
    ax_cdf.grid(False)
    ax_cdf.set_title('Cumulative Distribution Function (CDF)')

    # Add vertical line at rating
    ax_cdf.axvline(rating, color='gray', linestyle='--')

    # Shade the area below rating
    ax_cdf.fill_between(x_ratings, 0, cdf_ratings, where=(x_ratings <= rating), color='lightcoral', alpha=0.5, label=f'Rating < {rating}')

    # Add text for the cumulative probability of the rating less than or equal to rating
    ax_cdf.text(750, 0.90, f'$P(X \leq {rating})$ = {p_rating:.2f}', fontsize=12)

    ax_cdf.legend()

    plt.tight_layout(rect=[0, 0.03, 1, 0.95]); # Adjust layout to prevent title overlap
    plt.savefig(f"rating_distribution_{rating}.png", dpi=300)
    plt.show()

