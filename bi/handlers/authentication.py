import logging
from flask import abort, flash, redirect, render_template, request, url_for
import hashlib
import base64
import json
import time
import os
from flask_login import current_user, login_required, login_user, logout_user
from bi import __version__, limiter, models, settings, __DeepBI_version__
from bi.authentication import current_org, get_login_url, get_next_path
from bi.authentication.account import (
    BadSignature,
    SignatureExpired,
    send_password_reset_email,
    send_user_disabled_email,
    send_verify_email,
    validate_token,
)
from bi.handlers import routes
from bi.handlers.base import json_response, org_scoped_rule
from sqlalchemy.orm.exc import NoResultFound
from bi.handlers.language import get_config_language

logger = logging.getLogger(__name__)
lang = get_config_language()


def encrypt(text, key):
    key_hash = hashlib.sha256(key.encode()).digest()
    text_bytes = text.encode()
    encrypted_bytes = bytearray()

    for i in range(len(text_bytes)):
        encrypted_bytes.append(text_bytes[i] ^ key_hash[i % len(key_hash)])

    return base64.b64encode(encrypted_bytes).decode().replace("+", ".").replace("/", "+").replace("=", "_")


def decrypt(encrypted_text, key):
    encrypted_text = encrypted_text.replace("+", "/").replace(".", "+").replace("_", "=")
    key_hash = hashlib.sha256(key.encode()).digest()
    encrypted_bytes = base64.b64decode(encrypted_text.encode())
    decrypted_bytes = bytearray()

    for i in range(len(encrypted_bytes)):
        decrypted_bytes.append(encrypted_bytes[i] ^ key_hash[i % len(key_hash)])

    return decrypted_bytes.decode()


def make_secret(text):  # 默认来的时候是 data id, 回去的时候是 json 的 序列化字符串
    """
    import os
    import time
    """
    DB_API_SECRET_KEY = os.getenv("DB_API_SECRET_KEY")
    now_int_time = int(time.time())
    code_str = str(now_int_time) + "$$" + str(text) + "$$" + str(DB_API_SECRET_KEY)
    return encrypt(code_str, DB_API_SECRET_KEY)
    pass


def check_secret(data_id, secret):
    try:
        DB_API_SECRET_KEY = os.getenv("DB_API_SECRET_KEY")
        code_str = decrypt(secret, DB_API_SECRET_KEY)
        code_arr = code_str.split("$$")
        code_int_time = code_arr[0]
        code_data_id = code_arr[1]
        code_secret_key = code_arr[2]
        if DB_API_SECRET_KEY != code_secret_key:
            return False, 'Secret Error'
        now_int_time = time.time()
        if now_int_time - int(code_int_time) > 60:
            return False, 'Secret Overtime'
        if int(data_id) != int(code_data_id):
            return False, 'Database Error'
        return True, 'Success'
    except Exception as e:
        return False, "Error"
    pass


def get_google_auth_url(next_path):
    if settings.MULTI_ORG:
        google_auth_url = url_for(
            "google_oauth.authorize_org", next=next_path, org_slug=current_org.slug
        )
    else:
        google_auth_url = url_for("google_oauth.authorize", next=next_path)
    return google_auth_url


def render_token_login_page(template, org_slug, token, invite):
    try:
        user_id = validate_token(token)
        org = current_org._get_current_object()
        user = models.User.get_by_id_and_org(user_id, org)
    except NoResultFound:
        logger.exception(
            "Bad user id in token. Token=%s , User id= %s, Org=%s",
            token,
            user_id,
            org_slug,
        )
        return (
            render_template(
                "error.html",
                error_message="Invalid invite link. Please ask for a new one.",
            ),
            400,
        )
    except (SignatureExpired, BadSignature):
        logger.exception("Failed to verify invite token: %s, org=%s", token, org_slug)
        return (
            render_template(
                "error.html",
                error_message="Your invite link has expired. Please ask for a new one.",
            ),
            400,
        )

    if invite and user.details.get("is_invitation_pending") is False:
        return (
            render_template(
                "error.html",
                error_message=(
                    "This invitation has already been accepted. "
                    "Please try resetting your password instead."
                ),
            ),
            400,
        )

    status_code = 200
    if request.method == "POST":
        if "password" not in request.form:
            flash("Bad Request")
            status_code = 400
        elif not request.form["password"]:
            flash("Cannot use empty password.")
            status_code = 400
        elif len(request.form["password"]) < 6:
            flash("Password length is too short (<6).")
            status_code = 400
        else:
            if invite or user.is_invitation_pending:
                user.is_invitation_pending = False
            user.hash_password(request.form["password"])
            models.db.session.add(user)
            login_user(user)
            models.db.session.commit()
            return redirect(url_for("bi.index", org_slug=org_slug))

    google_auth_url = get_google_auth_url(url_for("bi.index", org_slug=org_slug))

    return (
        render_template(
            template,
            show_google_openid=settings.GOOGLE_OAUTH_ENABLED,
            google_auth_url=google_auth_url,
            show_saml_login=current_org.get_setting("auth_saml_enabled"),
            show_remote_user_login=settings.REMOTE_USER_LOGIN_ENABLED,
            show_ldap_login=settings.LDAP_LOGIN_ENABLED,
            org_slug=org_slug,
            user=user,
            lang=lang,
        ),
        status_code,
    )


@routes.route(org_scoped_rule("/data_source_info/<data_id>/<secret>"), methods=["GET"])  # add new api
def data_source_info(data_id, secret):
    if data_id is None or secret is None:
        print("need data_id")
        abort(404)
    is_pass, msg = check_secret(data_id, secret)
    if is_pass:
        data_id = int(data_id)
        # get database Info
        data_info = models.DataSource.get_by_id(data_id).get_private_options()
        # 返回数据
        data_info_str = json.dumps(data_info)
        data_info_str_code = make_secret(data_info_str)
        return json_response({
            "code": 200,
            "data": data_info_str_code
        })
    else:
        return json_response({
            "code": 201,
            "data": "",
            "msg": msg
        })
    pass


@routes.route(org_scoped_rule("/invite/<token>"), methods=["GET", "POST"])
def invite(token, org_slug=None):
    return render_token_login_page("invite.html", org_slug, token, True)


@routes.route(org_scoped_rule("/reset/<token>"), methods=["GET", "POST"])
def reset(token, org_slug=None):
    return render_token_login_page("reset.html", org_slug, token, False)


@routes.route(org_scoped_rule("/verify/<token>"), methods=["GET"])
def verify(token, org_slug=None):
    try:
        user_id = validate_token(token)
        org = current_org._get_current_object()
        user = models.User.get_by_id_and_org(user_id, org)
    except (BadSignature, NoResultFound):
        logger.exception(
            "Failed to verify email verification token: %s, org=%s", token, org_slug
        )
        return (
            render_template(
                "error.html",
                error_message="Your verification link is invalid. Please ask for a new one.",
            ),
            400,
        )

    user.is_email_verified = True
    models.db.session.add(user)
    models.db.session.commit()

    template_context = {"org_slug": org_slug} if settings.MULTI_ORG else {}
    next_url = url_for("bi.index", **template_context)

    return render_template("verify.html", next_url=next_url, lang=lang)


@routes.route(org_scoped_rule("/forgot"), methods=["GET", "POST"])
@limiter.limit(settings.THROTTLE_PASS_RESET_PATTERN)
def forgot_password(org_slug=None):
    if not current_org.get_setting("auth_password_login_enabled"):
        abort(404)

    submitted = False
    if request.method == "POST" and request.form["email"]:
        submitted = True
        email = request.form["email"]
        try:
            org = current_org._get_current_object()
            user = models.User.get_by_email_and_org(email, org)
            if user.is_disabled:
                send_user_disabled_email(user)
            else:
                send_password_reset_email(user)
        except NoResultFound:
            logging.error("No user found for forgot password: %s", email)

    return render_template("forgot.html", submitted=submitted, lang=lang)


@routes.route(org_scoped_rule("/verification_email/"), methods=["POST"])
def verification_email(org_slug=None):
    if not current_user.is_email_verified:
        send_verify_email(current_user, current_org)

    return json_response(
        {
            "message": "Please check your email inbox in order to verify your email address."
        }
    )


@routes.route(org_scoped_rule("/login"), methods=["GET", "POST"])
@limiter.limit(settings.THROTTLE_LOGIN_PATTERN)
def login(org_slug=None):
    # We intentionally use == as otherwise it won't actually use the proxy. So weird :O
    # noinspection PyComparisonWithNone
    if current_org == None and not settings.MULTI_ORG:
        return redirect("/setup")
    elif current_org == None:
        return redirect("/")

    index_url = url_for("bi.index", org_slug=org_slug)
    unsafe_next_path = request.args.get("next", index_url)
    next_path = get_next_path(unsafe_next_path)
    if current_user.is_authenticated:
        return redirect(next_path)

    if request.method == "POST" and current_org.get_setting("auth_password_login_enabled"):
        try:
            org = current_org._get_current_object()
            user = models.User.get_by_email_and_org(request.form["email"], org)
            if (
                user
                and not user.is_disabled
                and user.verify_password(request.form["password"])
            ):
                remember = "remember" in request.form
                login_user(user, remember=remember)
                return redirect(next_path)

            else:
                flash(lang['W_L']['email_or_password_error'])
        except NoResultFound:
            flash(lang['W_L']['email_or_password_error'])
    elif request.method == "POST" and not current_org.get_setting("auth_password_login_enabled"):
        flash(lang['W_L']['org_password_error'])

    google_auth_url = get_google_auth_url(next_path)

    return render_template(
        "login.html",
        org_slug=org_slug,
        next=next_path,
        email=request.form.get("email", ""),
        show_google_openid=settings.GOOGLE_OAUTH_ENABLED,
        google_auth_url=google_auth_url,
        show_password_login=current_org.get_setting("auth_password_login_enabled"),
        show_saml_login=current_org.get_setting("auth_saml_enabled"),
        show_remote_user_login=settings.REMOTE_USER_LOGIN_ENABLED,
        show_ldap_login=settings.LDAP_LOGIN_ENABLED,
        lang=lang,
    )

@routes.route(org_scoped_rule("/pretty_dashboard/<page>"))
def test(page):
    return render_template(str(page) + ".html")


@routes.route(org_scoped_rule("/logout"))
def logout(org_slug=None):
    logout_user()
    return redirect(get_login_url(next=None))


def base_href():
    if settings.MULTI_ORG:
        base_href = url_for("bi.index", _external=True, org_slug=current_org.slug)
    else:
        base_href = url_for("bi.index", _external=True)

    return base_href


def date_time_format_config():
    date_format = current_org.get_setting("date_format")
    date_format_list = set(["DD/MM/YY", "MM/DD/YY", "YYYY-MM-DD", settings.DATE_FORMAT])
    time_format = current_org.get_setting("time_format")
    time_format_list = set(["HH:mm", "HH:mm:ss", "HH:mm:ss.SSS", settings.TIME_FORMAT])
    return {
        "dateFormat": date_format,
        "dateFormatList": list(date_format_list),
        "timeFormatList": list(time_format_list),
        "dateTimeFormat": "{0} {1}".format(date_format, time_format),
    }


def number_format_config():
    return {
        "integerFormat": current_org.get_setting("integer_format"),
        "floatFormat": current_org.get_setting("float_format"),
    }


def client_config():
    if not current_user.is_api_user() and current_user.is_authenticated:
        client_config = {
            "newVersionAvailable": False,
            "version": __version__,
            "DeepBI_version": __DeepBI_version__
        }
    else:
        client_config = {}

    if (
        current_user.has_permission("admin")
        and current_org.get_setting("beacon_consent") is None
    ):
        client_config["showBeaconConsentMessage"] = True

    defaults = {
        "allowScriptsInUserInput": settings.ALLOW_SCRIPTS_IN_USER_INPUT,
        "showPermissionsControl": current_org.get_setting(
            "feature_show_permissions_control"
        ),
        "hidePlotlyModeBar": current_org.get_setting(
            "hide_plotly_mode_bar"
        ),
        "disablePublicUrls": current_org.get_setting("disable_public_urls"),
        "allowCustomJSVisualizations": settings.FEATURE_ALLOW_CUSTOM_JS_VISUALIZATIONS,
        "autoPublishNamedQueries": settings.FEATURE_AUTO_PUBLISH_NAMED_QUERIES,
        "extendedAlertOptions": settings.FEATURE_EXTENDED_ALERT_OPTIONS,
        "mailSettingsMissing": not settings.email_server_is_configured(),
        "dashboardRefreshIntervals": settings.DASHBOARD_REFRESH_INTERVALS,
        "queryRefreshIntervals": settings.QUERY_REFRESH_INTERVALS,
        "googleLoginEnabled": settings.GOOGLE_OAUTH_ENABLED,
        "ldapLoginEnabled": settings.LDAP_LOGIN_ENABLED,
        "pageSize": settings.PAGE_SIZE,
        "pageSizeOptions": settings.PAGE_SIZE_OPTIONS,
        "tableCellMaxJSONSize": settings.TABLE_CELL_MAX_JSON_SIZE,
    }

    client_config.update(defaults)
    client_config.update({"basePath": base_href()})
    client_config.update(date_time_format_config())
    client_config.update(number_format_config())

    return client_config


def messages():
    messages = []

    if not current_user.is_email_verified:
        messages.append("email-not-verified")

    if settings.ALLOW_PARAMETERS_IN_EMBEDS:
        messages.append("using-deprecated-embed-feature")

    return messages


@routes.route("/api/config", methods=["GET"])
def config(org_slug=None):
    return json_response(
        {"org_slug": current_org.slug, "client_config": client_config()}
    )


@routes.route(org_scoped_rule("/api/session"), methods=["GET"])
@login_required
def session(org_slug=None):
    if current_user.is_api_user():
        user = {"permissions": [], "apiKey": current_user.id}
    else:
        user = {
            "profile_image_url": current_user.profile_image_url,
            "id": current_user.id,
            "name": current_user.name,
            "email": current_user.email,
            "groups": current_user.group_ids,
            "permissions": current_user.permissions,
        }

    return json_response(
        {
            "user": user,
            "messages": messages(),
            "org_slug": current_org.slug,
            "client_config": client_config(),
        }
    )
