DROP FUNCTION IF EXISTS util.natgrid_en_to_latlon_M(phi double precision);

CREATE FUNCTION util.natgrid_en_to_latlon_M(phi double precision)
RETURNS double precision AS
$EN$
DECLARE
    b   CONSTANT double precision := 6356256.910;
    F0  CONSTANT double precision := 0.9996012717;
    phi0    CONSTANT double precision := radians(49.0);

    -- the following were computed by the accompanying bc script
    n1  CONSTANT double precision := 6364375.9402047103085325069229715636962153200803;
    n2  CONSTANT double precision := 31946.9744405285987286506652005581574659567102;
    n3  CONSTANT double precision := 33.4088711513091567080371230827923983957746;
    n4  CONSTANT double precision := 0.0434054617974685045678079437219673403390;
    M   double precision;

BEGIN
    M := (phi-phi0) * n1 - sin(phi-phi0) * cos(phi+phi0) * n2;
    M := M + sin(2.0*(phi-phi0)) * cos(2.0*(phi+phi0)) * n3;
    M := M - sin(3.0*(phi-phi0)) * cos(3.0*(phi+phi0)) * n4;
    RETURN M;
END;
$EN$
LANGUAGE plpgsql IMMUTABLE;

DROP FUNCTION IF EXISTS util.natgrid_en_to_latlon(
    E double precision,
    N double precision);

CREATE FUNCTION util.natgrid_en_to_latlon(
    E double precision,
    N double precision
    )
RETURNS TABLE (
    lat double precision,
    lon double precision
    )
AS
$EN$
DECLARE
    a   CONSTANT double precision := 6377563.396;
    b   CONSTANT double precision := 6356256.910;
    ee  CONSTANT double precision := 0.0066705397615975290736988693588125570545;
    F0  CONSTANT double precision := 0.9996012717;
    phi0    CONSTANT double precision := radians(49.0);
    lambda0 CONSTANT double precision := -radians(2.0);
    E0  CONSTANT double precision := 400000.0;
    N0  CONSTANT double precision := -100000.0;
    phi_prime   double precision;
    M       double precision;
    cpp     double precision;
    s2pp        double precision;
    tpp     double precision;
    t2pp        double precision;
    t4pp        double precision;
    t6pp        double precision;
    rho     double precision;
    nu      double precision;
    eta2        double precision;
    VII     double precision;
    VIII        double precision;
    IX      double precision;
    X       double precision;
    XI      double precision;
    XII     double precision;
    XIIA        double precision;
    phi     double precision;
    lambda      double precision;
BEGIN

    -- Uses the conversion formulae recommended by the
    -- Ordinance Survey for the UK National Grid (i.e. using
    -- the 1830 Airey ellipsoid). Not suitable for converting
    -- GRS80 (i.e. the WGS84 or GPS ellipsoid) co-ordinates.

    phi_prime := phi0 + (N - N0) / (a * F0);
    M := util.natgrid_en_to_latlon_M(phi_prime);
--  RAISE NOTICE  'phi_prime : %, M : %' , phi_prime, M;
    WHILE abs(N - N0 - M)>= 0.00001 LOOP
        phi_prime := phi_prime + (N - N0 - M) / (a * F0);
        M := util.natgrid_en_to_latlon_M(phi_prime);
--      RAISE NOTICE  'phi_prime : %, M : %' , phi_prime, M;
    END LOOP;

    -- These are conveniences to make the subsequent formulae
    -- much cleaner to read
    cpp := cos(phi_prime);
    s2pp := (sin(phi_prime))^2;
    tpp := tan(phi_prime);
    t2pp := (tan(phi_prime))^2.0;
    t4pp := (tan(phi_prime))^4.0;
    t6pp := (tan(phi_prime))^6.0;

    nu := a * F0 * power(1.0 - ee * s2pp, -0.5);
    rho := a * F0 * (1.0 - ee) * power(1.0 - ee * s2pp, -1.5);
    eta2 := nu / rho - 1.0;

--  RAISE NOTICE  'nu : %, rho : %, eta2 : %' , nu, rho, eta2;

    VII  := tpp / (2.0 * rho * nu);
    VIII := tpp / (24.0 * rho * (nu^3.0)) * (5.0 + 3.0*t2pp + eta2 - 9.0*eta2*t2pp);
    -- Note that while some publications are not clear on the matter, comparison with
    -- the worked example shows that the final term is definitely as shown.

--  RAISE NOTICE  'VII : %, VIII : %' , VII, VIII;

    IX := tpp / (720.0 * rho * (nu^5.0)) *
        (61.0 + 90.0*t2pp + 45.0*t4pp);
    X := 1.0 / (nu * cpp);

--  RAISE NOTICE  'IX : %, X : %' , IX, X;

    XI := 1.0 / (6.0 * nu^3 * cpp) * ( nu/rho + 2.0 * t2pp);
    XII := 1.0 / (120.0 * nu^5 * cpp) * ( 5.0 + 28.0 * t2pp + 24.0*t4pp);
    XIIA := 1.0 / (5040.0 * nu^7 * cpp) *
        (61.0 + 662.0*t2pp + 1320.0*t4pp + 720.0*t6pp);

--  RAISE NOTICE  'XI : %, XII : %, XIIA : %' , XI, XII, XIIA;

    phi := phi_prime - VII*(E - E0)^2.0 + VIII*(E - E0)^4.0 - IX*(E - E0)^6.0;
    lambda := lambda0 + X*(E - E0) - XI*(E - E0)^3.0 + XII*(E - E0)^5.0 - XIIA*(E - E0)^7.0;

    RETURN QUERY SELECT degrees(phi) AS lat, degrees(lambda) AS lon;

END;
$EN$
LANGUAGE plpgsql IMMUTABLE;
