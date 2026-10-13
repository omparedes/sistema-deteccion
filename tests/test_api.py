"""Scaffold: revision de fuente y peticion HTTP contra la aplicacion ASGI real."""
import ast
import unittest
import importlib.util
from pathlib import Path


class ApiScaffoldTest(unittest.TestCase):
    def test_health_http(self):
        from fastapi.testclient import TestClient
        path = Path(__file__).resolve().parents[1] / 'apps/api/app/main.py'
        spec = importlib.util.spec_from_file_location('api_health_under_test', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with TestClient(module.app) as client:
            response = client.get('/health')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {'status': 'ok', 'phase': 'scaffold'})
        self.assertTrue(response.headers['content-type'].startswith('application/json'))

    def test_app_has_health_endpoint_source(self):
        p = Path(__file__).resolve().parents[1] / 'apps' / 'api' / 'app' / 'main.py'
        tree = ast.parse(p.read_text(encoding='utf-8'))
        funcs = [n.name for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
        self.assertIn('health', funcs)


if __name__ == '__main__':
    unittest.main()
