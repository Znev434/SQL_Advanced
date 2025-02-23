# app.py
from flask import Flask, render_template, request, redirect, url_for
from database import (init_db, add_user, add_post, add_comment, add_like,
                     get_users, get_posts, get_comments, get_likes,
                     get_user_posts, get_post_comments, get_post_likes,
                     get_user_post_counts, get_most_commented_posts, get_top_likers,
                     get_logs, delete_inactive_users, delete_old_posts,
                     delete_orphan_comments, delete_orphan_likes, optimize_database,
                     export_users, export_posts, export_comments, export_likes, export_logs)

app = Flask(__name__)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/users", methods=["GET", "POST"])
def users():
    if request.method == "POST":
        username = request.form.get("username")
        email = request.form.get("email")
        if username and email:
            add_user(username, email)
        return redirect(url_for("users"))
    users = get_users()
    return render_template("users.html", users=users)

@app.route("/posts", methods=["GET", "POST"])
def posts():
    if request.method == "POST":
        user_id = request.form.get("user_id")
        content = request.form.get("content")
        if user_id and content:
            add_post(user_id, content)
        return redirect(url_for("posts"))
    posts = get_posts()
    return render_template("posts.html", posts=posts)

@app.route("/comments", methods=["GET", "POST"])
def comments():
    if request.method == "POST":
        user_id = request.form.get("user_id")
        post_id = request.form.get("post_id")
        content = request.form.get("content")
        if user_id and post_id and content:
            add_comment(user_id, post_id, content)
        return redirect(url_for("comments"))
    comments = get_comments()
    return render_template("comments.html", comments=comments)

@app.route("/likes", methods=["GET", "POST"])
def likes():
    if request.method == "POST":
        user_id = request.form.get("user_id")
        post_id = request.form.get("post_id")
        if user_id and post_id:
            add_like(user_id, post_id)
        return redirect(url_for("likes"))
    likes = get_likes()
    return render_template("likes.html", likes=likes)

@app.route("/analytics")
def analytics():
    user_post_counts = get_user_post_counts()
    most_commented_posts = get_most_commented_posts()
    top_likers = get_top_likers()
    logs = get_logs()
    return render_template("analytics.html",
                          user_post_counts=user_post_counts,
                          most_commented_posts=most_commented_posts,
                          top_likers=top_likers,
                          logs=logs)

@app.route("/management", methods=["GET", "POST"])
def management():
    if request.method == "POST":
        action = request.form.get("action")
        if action == "delete_inactive_users":
            delete_inactive_users()
        elif action == "delete_old_posts":
            delete_old_posts()
        elif action == "delete_orphan_comments":
            delete_orphan_comments()
        elif action == "delete_orphan_likes":
            delete_orphan_likes()
        elif action == "optimize_database":
            optimize_database()
        elif action == "export_users":
            export_users()
        elif action == "export_posts":
            export_posts()
        elif action == "export_comments":
            export_comments()
        elif action == "export_likes":
            export_likes()
        elif action == "export_logs":
            export_logs()
        return redirect(url_for("management"))
    return render_template("management.html")

init_db()

if __name__ == "__main__":
    app.run(debug=True)