package utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

import org.junit.Before;
import org.junit.Test;

/**
 * Basically a copy of the EqualsHashCodeTestCase but with Junit 1.5 annotations
 * instead of extending TestCase.
 * 
 * This should be used whenever you are overriding .equals and .hashcode.
 * 
 * @author graywatson
 */
public abstract class LocalEqualsHashCodeTest<T> {

    private Object eq1;
    private Object eq2;
    private Object eq3;
    private Object neq;
    private static final int NUM_ITERATIONS = 100;

    /**
     * Creates and returns an instance of the class under test.
     *
     * @return  a new instance of the class under test; each object returned
     * from this method should compare equal to each other.
     * @throws  Exception
     */
    protected abstract T createInstance() throws Exception;

    /**
     * Creates and returns an instance of the class under test.
     *
     * @return a new instance of the class under test; each object returned
     * from this method should compare equal to each other, but not to the
     * objects returned from {@link #createInstance() createInstance}.
     * @throws Exception
     */
    protected abstract T createNotEqualInstance() throws Exception;

    /**
     * Sets up the test fixture.
     *
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {

        eq1 = createInstance();
        eq2 = createInstance();
        eq3 = createInstance();
        neq = createNotEqualInstance();

        // We want these assertions to yield errors, not failures.
        try {
            assertNotNull("createInstance() returned null", eq1);
            assertNotNull("2nd createInstance() returned null", eq2);
            assertNotNull("3rd createInstance() returned null", eq3);
            assertNotNull("createNotEqualInstance() returned null", neq);

            assertNotSame(eq1, eq2);
            assertNotSame(eq1, eq3);
            assertNotSame(eq1, neq);
            assertNotSame(eq2, eq3);
            assertNotSame(eq2, neq);
            assertNotSame(eq3, neq);

            assertEquals(
                    "1st and 2nd equal instances of different classes",
                    eq1.getClass(),
                    eq2.getClass());
            assertEquals(
                    "1st and 3rd equal instances of different classes",
                    eq1.getClass(),
                    eq3.getClass());
            assertEquals(
                    "1st equal instance and not-equal instance of different classes",
                    eq1.getClass(),
                    neq.getClass());
        } catch (AssertionError ex) {
            throw new IllegalArgumentException(ex.getMessage());
        }
    }

    /**
     * Tests whether <code>equals</code> holds up against a new
     * <code>Object</code> (should always be <code>false</code>).
     */
    @Test
    public final void testEqualsAgainstNewObject() {
        Object o = new Object();

        assertFalse("new Object() vs. 1st", eq1.equals(o));
        assertFalse("new Object() vs. 2nd", eq2.equals(o));
        assertFalse("new Object() vs. 3rd", eq3.equals(o));
        assertFalse("new Object() vs. not-equal", neq.equals(o));
    }

    /**
     * Tests whether <code>equals</code> holds up against <code>null</code>.
     */
    @Test
    public final void testEqualsAgainstNull() {
        assertFalse("null vs. 1st", eq1.equals(null));
        assertFalse("null vs. 2nd", eq2.equals(null));
        assertFalse("null vs. 3rd", eq3.equals(null));
        assertFalse("null vs. not-equal", neq.equals(null));
    }

    /**
     * Tests whether <code>equals</code> holds up against objects that should
     * not compare equal.
     */
    @Test
    public final void testEqualsAgainstUnequalObjects() {
        assertFalse("1st vs. not-equal", eq1.equals(neq));
        assertFalse("2nd vs. not-equal", eq2.equals(neq));
        assertFalse("3rd vs. not-equal", eq3.equals(neq));

        assertFalse("not-equal vs. 1st", neq.equals(eq1));
        assertFalse("not-equal vs. 2nd", neq.equals(eq2));
        assertFalse("not-equal vs. 3rd", neq.equals(eq3));
    }

    /**
     * Tests whether <code>equals</code> is <em>consistent</em>.
     */
    @Test
    public final void testEqualsIsConsistentAcrossInvocations() {
        for (int i = 0; i < NUM_ITERATIONS; ++i) {
            testEqualsAgainstNewObject();
            testEqualsAgainstNull();
            testEqualsAgainstUnequalObjects();
            testEqualsIsReflexive();
            testEqualsIsSymmetricAndTransitive();
        }
    }

    /**
     * Tests whether <code>equals</code> is <em>reflexive</em>.
     */
    @Test
    public final void testEqualsIsReflexive() {
        assertEquals("1st equal instance", eq1, eq1);
        assertEquals("2nd equal instance", eq2, eq2);
        assertEquals("3rd equal instance", eq3, eq3);
        assertEquals("not-equal instance", neq, neq);
    }

    /**
     * Tests whether <code>equals</code> is <em>symmetric</em> and
     * <em>transitive</em>.
     */
    @Test
    public final void testEqualsIsSymmetricAndTransitive() {
        assertEquals("1st vs. 2nd", eq1, eq2);
        assertEquals("2nd vs. 1st", eq2, eq1);

        assertEquals("1st vs. 3rd", eq1, eq3);
        assertEquals("3rd vs. 1st", eq3, eq1);

        assertEquals("2nd vs. 3rd", eq2, eq3);
        assertEquals("3rd vs. 2nd", eq3, eq2);
    }

    /**
     * Tests the <code>hashCode</code> contract.
     */
    @Test
    public final void testHashCodeContract() {
        assertEquals("1st vs. 2nd", eq1.hashCode(), eq2.hashCode());
        assertEquals("1st vs. 3rd", eq1.hashCode(), eq3.hashCode());
        assertEquals("2nd vs. 3rd", eq2.hashCode(), eq3.hashCode());
    }

    /**
     * Tests the consistency of <code>hashCode</code>.
     */
    @Test
    public final void testHashCodeIsConsistentAcrossInvocations() {
        int eq1Hash = eq1.hashCode();
        int eq2Hash = eq2.hashCode();
        int eq3Hash = eq3.hashCode();
        int neqHash = neq.hashCode();

        for (int i = 0; i < NUM_ITERATIONS; ++i) {
            assertEquals("1st equal instance", eq1Hash, eq1.hashCode());
            assertEquals("2nd equal instance", eq2Hash, eq2.hashCode());
            assertEquals("3rd equal instance", eq3Hash, eq3.hashCode());
            assertEquals("not-equal instance", neqHash, neq.hashCode());
        }
    }
}