import logging
from flask import g,abort, flash, redirect, render_template, request, url_for,session,jsonify
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
from bi.handlers.feishu_auth import Auth
from sqlalchemy.exc import IntegrityError
from passlib.apps import custom_app_context as pwd_context
from bi.models import Group, Organization, User, db, DataSourceFile


logger = logging.getLogger(__name__)
lang = get_config_language()

USER_INFO_KEY = "UserInfo"

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
    
# 业务逻辑类
class Biz(object):
    @staticmethod
    def home_handler():
        # 主页加载流程
        return Biz._show_user_info()

    @staticmethod
    def login_handler():
        return render_template("login.html", user_info={"name": "unknown"}, login_info="needLogin")

    # @staticmethod
    # def login_failed_handler(err_info):
        # 出错后的页面加载流程
        # return Biz._show_err_info(err_info)

    @staticmethod
    def _show_user_info():
        return render_template("login.html", user_info=session[USER_INFO_KEY], login_info="alreadyLogin")

@routes.route(org_scoped_rule("/callback"), methods=["GET"])
def callback(org_slug=None):
    # 获取 user info
    APP_ID = os.getenv("APP_ID")
    APP_SECRET = os.getenv("APP_SECRET")
    FEISHU_HOST = os.getenv("FEISHU_HOST")
    auth = Auth(FEISHU_HOST, APP_ID, APP_SECRET)
    # 拿到前端传来的临时授权码 Code
    code = request.args.get("code")
    # 先获取 user_access_token
    auth.authorize_user_access_token(code)
    # 再获取 user info
    user_info = auth.get_user_info()
    # 将 user info 存入 session
    session[USER_INFO_KEY] = user_info
    return jsonify(user_info)

@routes.route(org_scoped_rule("/get_appid"), methods=["GET"])
def get_appid(org_slug=None):
    # 获取 appid
    return jsonify(
        {
            "appid": os.getenv("APP_ID")
        }
    )

def create_user(org_name, org_slug, email, password):
    default_org = Organization(name=org_name, slug=org_slug, settings={})
    g.org = default_org
    # default_org = current_org._get_current_object()
    admin_group = Group(
        name="admin1",
        permissions=["admin", "super_admin"],
        org=default_org,
        type=Group.BUILTIN_GROUP,
    )
    default_group = Group(
        name="default1",
        permissions=Group.DEFAULT_PERMISSIONS,
        org=default_org,
        type=Group.BUILTIN_GROUP,
    )

    db.session.add_all([default_org, admin_group, default_group])
    db.session.commit()

    print("新的租户：", default_org)
    print("admin_group===",admin_group)
    print("admin_group.id===",admin_group.id)
    print("default_group===",default_group)
    print("default_group.id===",default_group.id)

    user = User(
        org=default_org,
        name=email,
        email=email,
        group_ids=[admin_group.id, default_group.id],
    )
    user.hash_password(password)

    db.session.add(user)
    db.session.commit()
    print("新的user++++: ", user)
    return default_org, user

@routes.route(org_scoped_rule("/login"), methods=["GET","POST"])
# @limiter.limit(settings.THROTTLE_LOGIN_PATTERN)
def login(org_slug=None):
    index_url = url_for("bi.index", org_slug=org_slug)
    unsafe_next_path = request.args.get("next", index_url)
    next_path = get_next_path(unsafe_next_path)
    print("org_slug: ", org_slug)
    print("next_path: ", next_path)
    if request.method == "GET":
        print("GET---GET")
        print("登录状态：", current_user.is_authenticated)
        if current_user.is_authenticated:
            print("已登录")
            return redirect(next_path)
        else:
            print("未登录")
            logging.info("need to get user information")
            return Biz.login_handler()
    elif request.method == "POST":
        user_platform = request.form["platform"]
        print("user_platform: ", user_platform)
        open_id = session[USER_INFO_KEY]["open_id"]
        user_email = open_id + "@" + user_platform + ".cn"
        if current_user.is_authenticated:
            print("current_user.is_authenticated")
            return redirect(next_path)
       
        try:
            user = models.User.query.filter(models.User.email == user_email).first()
            if user is not None:
                print("User [%s] is already exists." % user_email)
            else:
                # 查询组织
                org = models.Organization.get_by_slug(open_id)
                print("查询租户：", org)
                if org is None:
                    print("未查询到租户：", open_id)
                    org, user = create_user(user_platform, open_id, user_email, open_id)
            print("login...")
            login_user(user, remember=True)
            print("current_user.is_authenticated===",current_user.is_authenticated)
            print("login_user----",next_path)
            print("Session:", session)
            return redirect(next_path)
        except Exception as e:
            logger.error(f"Error creating user: {e}")
            abort(500, description="Error creating user")




@routes.route(org_scoped_rule("/pretty_dashboard/<page>"))
def test(page):
    return render_template(str(page) + ".html")


@routes.route(org_scoped_rule("/logout"))
def logout(org_slug=None):
     # 清除session中的用户信息
    session.pop(USER_INFO_KEY, None)
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
def sessions(org_slug=None):
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
