# Book Recommendation System using Streamlit

## Description

This is a simple recommendation system implemented in Streamlit, utilizing Selenium for web scraping to gather additional information about recommended items.

## Installation

1. Clone the repository:

```bash
git clone https://github.com/NguyenAnhKietUIT/IS254.O11.git
cd IS254.O11
```

2. Install the required dependencies:

```bash
pip install streamlit selenium
```

3. Run the Streamlit app:

```
cd web
streamlit run app.py
```

4. Open your browser (Firefox) and go to http://localhost:8501 to interact with the recommendation system.

## File Structure

- `app.py`: The main application file containing the Streamlit code.
- `regression_book_recommend_list.pkl`: Pickle file for regression book recommendation list.
- `regression_link_recommend_list.pkl`: Pickle file for regression link recommendation list.
- `classification_book_recommend_list.pkl`: Pickle file for classification book recommendation list.
- `classification_link_recommend_list.pkl`: Pickle file for classification link recommendation list.

## Features

- `Model Selection`: Choose between Regression and Classification models.
- `Category Selection`: Choose between recommending Books or Pages.
- `Item Selection`: Select a specific item from the dropdown list.
- `Show Recommendation`: Click the button to display recommended items based on the selected model and category.

## Code Explanation
![Flow](images/DSS_Flow_Code-en.drawio.png)

The main application logic is in the `app.py` file. It uses Streamlit for the web interface, Selenium for web scraping, and Pickle for loading pre-trained recommendation models.

### Dependencies:

- `pickle`: For loading pre-trained models.
- `streamlit`: For building the web application.
- `selenium`: For web scraping and interaction.
- `ctypes.wintypes.SERVICE_STATUS_HANDLE`: Part of the Windows API.
- `os`: Operating system interaction.
- `firefox`: Web browser for headless operation.

### Running the Application:

- The application initializes a headless Firefox browser using Selenium and defines CSS styles for better presentation. It provides a function `find_similar_items` to fetch and display information about recommended items.
- The user can choose the model type (`Regression` or `Classification`), select the category (`Book` or `Page`), and pick a specific item. Clicking the "Show Recommendation" button triggers the recommendation display.

## Acknowledgments

- [Streamlit](https://streamlit.io/)
- [Selenium](https://www.selenium.dev/)
- [wikipedia-data-science](https://github.com/WillKoehrsen/wikipedia-data-science)

Feel free to explore and enhance the project as needed!
