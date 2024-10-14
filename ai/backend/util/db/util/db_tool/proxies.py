class ProxyManager:
    def __init__(self):
        self.toggle = True  # 初始为有代理

    def get_proxies(self, region):
        proxies = {
            "http": "http://192.168.2.165:7890",
            "https": "https://daili.deepbi.com"  # https://daili2.deepbi.com
        }
        if region == "JP":
            if self.toggle:
                self.toggle = False
                print("有代理")
                return proxies
            else:
                self.toggle = True
                print("无代理")
                return {}
        else:
            return {}

# 示例用法
if __name__ == "__main__":
    proxy_manager = ProxyManager()
    print(proxy_manager.get_proxies("JP"))  # 返回 {}
    print(proxy_manager.get_proxies("JP"))  # 返回 proxies
    print(proxy_manager.get_proxies("JP"))
    print(proxy_manager.get_proxies("JP"))
    print(proxy_manager.get_proxies("JP"))
