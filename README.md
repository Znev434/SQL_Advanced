# Social Platform

A simple web application built with Flask and SQLite, designed to manage users, posts, comments, and likes in a social media-like environment. This project showcases skills in relational database management (SQLite) and web development (Flask).

## Features

- **User Management**: Add and display users with unique usernames and emails.
- **Posts**: Create and view posts linked to users.
- **Comments**: Add and display comments for posts.
- **Likes**: Record and view post likes with a uniqueness constraint.
- **Analytics**: Execute advanced SQL queries, including user post counts, most commented posts, and top likers.
- **Data Management**: Delete inactive users, old posts, and orphaned comments/likes; optimize the database.
- **Data Export**: Export tables (users, posts, comments, likes, logs) to CSV files.

## Technologies

- **Python**: Programming language.
- **SQLite**: Relational database featuring tables, foreign keys, indexes, and transactions.
- **Flask**: Web framework for building the application.
- **HTML/CSS**: Used for user interface design with forms and styling.
- **Jinja2**: Templating engine for dynamic data rendering.

## Project Structure

```plaintext
project/
├── static/
│   └── style.css       # Interface styling
├── templates/
│   ├── index.html      # Home page
│   ├── users.html      # Users page
│   ├── posts.html      # Posts page
│   ├── comments.html   # Comments page
│   ├── likes.html      # Likes page
│   ├── analytics.html  # Analytics page
│   └── management.html # Management page
├── database.py         # Database logic
├── app.py              # Flask application
└── database.sqlite     # Database file
```

## What I Achieved

### SQLite
- Designed a relational database with 5 tables, incorporating foreign keys and indexes for optimized queries.
- Implemented CRUD operations with robust transaction handling and error management.
- Developed advanced SQL queries for data analysis (e.g., JOIN, GROUP BY, HAVING).
- Added functionality for exporting data to CSV and cleaning up the database (e.g., removing old posts).

### Flask
- Built a web application with a clear separation of concerns between database logic (database.py) and application logic (app.py).
- Created dynamic endpoints with HTML forms for user input.
- Integrated analytics and management pages that leverage comprehensive SQL functionalities.
- Applied professional CSS styling to deliver an appealing user interface.

## Example Usage
- **Add a user**: Visit /users and add a user (e.g., "user1", "user1@example.com").
- **Create a post**: Visit /posts and create a post (e.g., User ID 1, "Test post").
- **Comment on a post**: Visit /comments to add a comment (e.g., User ID 1, Post ID 1, "Test comment").
- **Like a post**: Visit /likes to like a post (e.g., User ID 1, Post ID 1).
- **View analytics**: Check /analytics to view analytical data.
- **Manage the database**: Use /management to perform administrative tasks.

## Download
Download the entire project as a ZIP file by clicking the green "Code" button on GitHub and selecting "Download ZIP".

## Author
Michał Wenz