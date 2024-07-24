from flask import g, redirect, render_template, request, url_for

from flask_login import login_user
from bi import settings
from bi.authentication.org_resolving import current_org
from bi.handlers.base import routes
from bi.models import Group, Organization, User, db, DataSourceFile
from wtforms import BooleanField, Form, PasswordField, StringField, validators
from wtforms.fields.html5 import EmailField
from bi.handlers.language import get_config_language

lang = get_config_language()


class SetupForm(Form):
    name = StringField(lang['W_L']['user_name'], validators=[validators.InputRequired()])
    email = EmailField(lang['W_L']['email'], validators=[validators.Email()])
    password = PasswordField(lang['W_L']['password'], validators=[validators.Length(6)])
    org_name = StringField(lang['W_L']['organization'], validators=[validators.InputRequired()])
    security_notifications = BooleanField()
    newsletter = BooleanField()


def create_org(org_name, user_name, email, password):
    default_org = Organization(name=org_name, slug="default", settings={})
    admin_group = Group(
        name="admin",
        permissions=["admin", "super_admin"],
        org=default_org,
        type=Group.BUILTIN_GROUP,
    )
    default_group = Group(
        name="default",
        permissions=Group.DEFAULT_PERMISSIONS,
        org=default_org,
        type=Group.BUILTIN_GROUP,
    )

    db.session.add_all([default_org, admin_group, default_group])
    db.session.commit()

    user = User(
        org=default_org,
        name=user_name,
        email=email,
        group_ids=[admin_group.id, default_group.id],
    )
    user.hash_password(password)

    db.session.add(user)
    db.session.commit()

    return default_org, user


@routes.route("/setup", methods=["GET", "POST"])
def setup():
    if current_org != None or settings.MULTI_ORG:
        return redirect("/")

    form = SetupForm(request.form)
    form.newsletter.data = True
    form.security_notifications.data = True

    if request.method == "POST" and form.validate():
        default_org, user = create_org(
            form.org_name.data, form.name.data, form.email.data, form.password.data
        )

        g.org = default_org
        login_user(user)
        #  init user test csv file.
        csv_file = {
            'order_detail': 'order_details.csv',
            'order_list': 'order_list.csv',
            'sales_target': 'sales_target.csv'
        }
        user_id = user.id
        org_id = default_org.id
        for key in csv_file.keys():
            new_filename = csv_file[key]
            result = DataSourceFile(
                user_id=user_id,
                org_id=org_id,
                source_name=key,
                file_name=new_filename,
                is_use=True,
                file_type="CSV",
            )
            db.session.add(result)
        db.session.commit()
        #  init over
        return redirect(url_for("bi.index", org_slug=None))

    return render_template("setup.html", form=form, lang=lang)
